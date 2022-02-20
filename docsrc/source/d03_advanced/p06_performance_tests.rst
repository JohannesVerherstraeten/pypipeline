Performance tests
=================

.. warning::
    The real-life usecase that filled this page is removed for IP-/NDA reasons.

    To give a rough idea, scaling up twice gave a throughput increase of +50% for a heavy GPU-load task, and +80% for
    a heavy CPU-load task. The difference between scaling up with multithreading or Ray was small, but multithreading
    performed slightly better in the GPU-load task, and Ray performed better in the CPU-load task.

    Note that those tests were performed on a node with limited hardware.


.. note::
    TODO add new content.

    - throughput of a pipeline that only passes data and doesn't perform operations on it -> quantify any overhead
      costs.
    - throughput of a real-life usecase:
        * without PyPipeline
        * with PyPipeline, without scaling -> what overhead does PyPipeline add when not using scaling functionality?
        * with PyPipeline, with scaling -> plot throughput as a function of how many times a cell is scaled up.
    - test on usecases with heavy CPU/GPU/IO load to visualize the advantages of the different scaling
      methods for each type of load.
