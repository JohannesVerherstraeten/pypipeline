# Copyright (C) 2021  Johannes Verherstraeten
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
# 
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see https://www.gnu.org/licenses/agpl-3.0.en.html
"""
TODO what happens to a FastAPIServer call if somewhere in the pipeline an exception is raised?
TODO what happens if that exception occurs inside one of the multiple clones of a ScalableCell?
"""

import asyncio
from typing import Optional, TypeVar, TYPE_CHECKING, List, Tuple, Type
from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from threading import Lock, Event, Condition

from pypipeline.cell import ScalableCell
from pypipeline.cellio import IInput, IOutput
from pypipeline_serve.endpoints.aendpointfactory import AEndpointFactory
from pypipeline_serve.endpoints.utils import create_pydantic_model, decorate_as_form, EncodingManager, set_values, \
    get_values, create_pydantic_model_recursively

if TYPE_CHECKING:
    from pypipeline_serve.fastapiserver import FastAPIServer


EncodedType = TypeVar("EncodedType")
IOType = TypeVar("IOType")


class ExecutionEndpointsFactory(AEndpointFactory):

    TAG = "Execution"

    def __init__(self, input_as_form_data: bool = False):
        super(ExecutionEndpointsFactory, self).__init__()
        self.__input_as_form_data = input_as_form_data

    def create_endpoints(self,
                         app: FastAPI,
                         cell: "FastAPIServer",
                         encoding_manager: Optional[EncodingManager] = None) -> None:
        self._create_pull_endpoint(app, cell, encoding_manager)

    def _create_pull_endpoint(self,
                              app: FastAPI,
                              cell: "FastAPIServer",
                              encoding_manager: Optional[EncodingManager] = None) -> None:
        PullModelIn, PullModelOut = self._create_pydantic_models(cell, encoding_manager)
        set_value_lock_to_improve = Lock()
        pull_condition = Condition()
        waiters: List[Event] = []

        def _is_my_turn(waiter_) -> bool:
            waiters[0].set()
            pull_condition.notify()
            return waiter_.is_set()

        # TODO input validation
        # TODO better exception handling
        def execution_body_sync(input_values: PullModelIn) -> PullModelOut:
            try:
                with set_value_lock_to_improve:
                    self.logger.debug(f"FastAPIServer execution body set values")
                    set_values(cell, input_values, encoding_manager)
                    waiter = Event()
                    waiters.append(waiter)

                # TODO this synchronization doesn't work well with downscaling a scalable cell
                #  ... but downscaling a scalable cell doesn't do well with an HTTP API in general. -> don't allow yet
                with pull_condition:
                    self.logger.debug(f"FastAPIServer execution body trying to pull")
                    pull_condition.wait_for(lambda: _is_my_turn(waiter))
                    waiters.pop(0)
                    self.logger.debug(f"FastAPIServer execution body pull")
                    try:
                        cell.pull()
                        self.logger.debug(f"FastAPIServer execution body get values")
                        output_values = get_values(cell, PullModelOut, encoding_manager) if PullModelOut is not None else None
                        self.logger.debug(f"FastAPIServer execution body done. output values = {output_values}")
                    finally:
                        pull_condition.notify()

            except Exception as e:
                self.logger.warning(f"Execution body errored: ")
                self.logger.error(e, exc_info=True)
                raise HTTPException(500, detail=f"{type(e)}: {str(e)}")
            return output_values

        async def execution_body_async(input_values: PullModelIn) -> PullModelOut:
            loop = asyncio.get_running_loop()

            ## Options:
            # 1. Run in the default loop's executor:
            pool = None
            # # 2. Run in a custom thread pool:
            # with concurrent.futures.ThreadPoolExecutor() as pool:
            # # 3. Run in a custom process pool:
            # !!! in this case multiprocessing locks should be used in the execution body
            # with concurrent.futures.ProcessPoolExecutor() as pool:

            result = await loop.run_in_executor(pool, execution_body_sync, input_values)
            self.logger.warning(f"Result: {result}")
            return result

        has_scalable_cell = self.__server_has_internal_scalable_cell(cell)

        async def execution_body(input_values: PullModelIn) -> PullModelOut:
            if has_scalable_cell:       # use async function definition
                return await execution_body_async(input_values)
            else:
                return execution_body_sync(input_values)

        if self.__input_as_form_data:
            PullModelFormIn = decorate_as_form(PullModelIn)     # Adds the .as_form() class method

            @app.post("/pull", response_model=PullModelOut, tags=[self.TAG],
                      description=f"Execute `{cell.get_name()}` with the given inputs. ")
            async def execute(input_values: PullModelFormIn = Depends(PullModelFormIn.as_form)) -> PullModelOut:
                return await execution_body(input_values)

        else:
            @app.post("/pull", response_model=PullModelOut, tags=[self.TAG],
                      description=f"Execute `{cell.get_name()}` with the given inputs. ")
            async def execute(input_values: PullModelIn) -> PullModelOut:
                return await execution_body(input_values)

    @staticmethod
    def _create_pydantic_models(cell: "FastAPIServer",
                                encoding_manager: Optional[EncodingManager] = None) -> Tuple[Optional[Type[BaseModel]],
                                                                                             Optional[Type[BaseModel]]]:
        pydantic_inputs: List[IInput] = list(cell.get_inputs())
        PullModelIn = create_pydantic_model_recursively("PullModelIn", cell, pydantic_inputs, encoding_manager)

        pydantic_outputs: List[IOutput] = list(cell.get_outputs())
        PullModelOut = create_pydantic_model_recursively("PullModelOut", cell, pydantic_outputs, encoding_manager)

        return PullModelIn, PullModelOut

    @staticmethod
    def __server_has_internal_scalable_cell(server: "FastAPIServer") -> bool:
        internal_cells = server.get_internal_cells_recursively()
        has_scalable_cell = False
        for cell in internal_cells:
            if isinstance(cell, ScalableCell):
                has_scalable_cell = True
                break
        return has_scalable_cell
