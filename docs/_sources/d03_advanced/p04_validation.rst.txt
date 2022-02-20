Validation
==========


Input validation
----------------

You can add input validation to your cells:


.. code-block:: python
    :emphasize-lines: 6,8

    class ExampleCell(ASingleCell):

        def __init__(self, parent_cell: "Optional[ACompositeCell]", name: str):
            super(ExampleCell, self).__init__(parent_cell, name=name)
            self.param_batch_size: RuntimeParameter[int] = RuntimeParameter(self, "batch_size",
                                                                            validation_fn=self.is_valid_batch_size)
            self.input_nonempty_list: Input[List[Any]] = Input(self, "nonempty_list",
                                                               validation_fn=self.is_valid_nonempty_list)

        def is_valid_batch_size(self, batch_size: int) -> bool:
            return isinstance(batch_size, int) and batch_size > 0

        def is_valid_nonempty_list(self, nonempty_list: List[Any]) -> bool:
            return isinstance(nonempty_list, list) and len(nonempty_list) > 0

        def _on_pull(self) -> None:
            self.param_batch_size.pull()
            self.input_nonempty_list.pull()


If an input value arrives that doesn't satisfy the validation function, an `InvalidInputException` is raised.


Pipeline validation
-------------------

Every object in PyPipeline has an `assert_is_valid()` method. This method validates whether all attributes of the
object are "proper", that is:

- the attribute satisfies the `object.can_have_as_<attribute>(attr)` constraint. Ex: if cell A has a parent cell,
  the following condition must satisfy: `assert cell_a.can_have_as_parent_cell(cell_a.get_parent_cell())`.
- if the attribute is another PyPipeline object that holds a reference to the attribute, the mutual references are
  correct. Ex: the parent cell of cell A, has cell A as its child cell:
  `assert cell_a.get_parent_cell().has_as_internal_cell(cell_a)`.

The `assert_is_valid()` method returns without effect if the pipeline is valid, and raises an `InvalidStateException`
otherwise. When called on a fully built pipeline, the method will call itself recursively on every object in
the full pipeline.

This method should **never raise an exception**, as this would indicate a bug in PyPipeline. After all, by using only
public methods, a user should not be able to bring a pipeline into an invalid state.


Pipeline is-deployable check
----------------------------

The `cell.is_deployable()` method performs some additional checks on top of general validation.
After all, a pipeline can be valid, but this doesn't necessarily mean it can be executed. For example, if one
of the inputs of a cell doesn't have an incoming connection, or if the topology of a pipeline cannot be determined,
the pipeline cannot be executed.

This check is performed when deploying a cell or pipeline.
