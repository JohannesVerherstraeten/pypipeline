Minimal example
===============

Let's start with defining the most minimal cell.

First we import the ``ASingleCell`` abstract class. This is the class of which most of our self-defined cells
will inherit. It serves as a base for the most basic type of cells.

.. code-block:: python

    from pypipeline.cell import ASingleCell


What follows is the definition of our hello world cell. The ``pull()`` method is the only method that a cell is
required to implement. It is called when the cell gets executed, and should contain your algorithm code.

.. code-block:: python

    class HelloWorldCell(ASingleCell):

        def _on_pull(self) -> None:
            print("Hello world!")


Cells that are created take two arguments: a parent cell and a name. The parent cell is optional, it is used when we
want to nest cells inside a pipeline object for example. We'll keep it ``None`` for now.
The name of the cell serves as a unique identifier. This name will get more important when building (and debugging)
more complex pipelines.

.. code-block:: python

    cell = HelloWorldCell(parent_cell=None, name="my_name")


Before starting to pull (=execute) our cell, we first need to deploy it. In this particularly easy case, deployment
is not strictly needed as the cell doesn't need to acquire specific resources, but it is good practice to do it anyway.
It signals PyPipeline that you're ready building your cell/pipeline, and that you can move on to the execution phase.

.. code-block:: python

    cell.deploy()


Executing the cell is now as easy as:

.. code-block:: python

    for i in range(2):
        cell.pull()


Output:

.. code-block:: text

    Hello world!
    Hello world!

That's it! You successfully executed your first PyPipeline program. You can now pull your cell as much as you want,
and observe its well-known output.
