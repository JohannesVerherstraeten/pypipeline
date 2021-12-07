PyPipeline
==========

*Encapsulate computations, combine them to algorithms, enable pipeline parallelism and scale up.*

About
-----
**PyPipeline provides a toolbox for building and optimizing the throughput of your data processing pipelines.**

The target usecases of PyPipeline are algorithms that process batches or streams of data. Example usecases are
image- and videostream analysis, financial stock analysis, ... It is specifically tailed for training and inference
of AI-based algorithms, but is in no way limited to the AI domain.

It allows you to:
 * **Build your algorithm** as a pipeline of multiple steps.
 * **Parallelize** (parts of) your pipeline using multithreading and/or multiprocessing (`Ray <https://ray.io/>`_),
   **without having to care about threadsafety or synchronization**. Switching between the two parallelization methods
   is as easy as changing one parameter.
 * **Scale up** parts of your pipeline that are **throughput bottlenecks**. The incoming data is divided over
   multiple clones of these bottlenecks, all working in parallel.
 * **Process your data in order of arrival**, even when upstream parts of your pipeline are parallelized and scaled up.
   (Ordered processing is important if some parts of your pipelines are stateful.)

PyPipeline is built with an emphasis on good object-oriented software design, with as primary purpose making the
code easy to understand, extend and maintain. Its goal is to serve as a solid foundation to deploy and optimize
your applications in production.

PyPipeline is licensed under the
`GNU Affero General Public License v3 <https://www.gnu.org/licenses/agpl-3.0.en.html>`_.
It is actively being developed: currently I (Johannes) have 2 days a week reserved for PyPipeline development.
It is in alpha state, meaning that the interfaces are pretty stable but not yet fixed, and bugs may still occur.
Please don't hesitate to submit feature requests, file bug reports, ask questions (for now, you can use Github
Issues for them all). I'll try to answer ASAP.


High-level overview
-------------------

The idea of PyPipeline is to break complex algorithms working on datastreams into multiple pieces called cells.
Each cell is a computational unit with inputs and/or outputs. These inputs and outputs can be connected with each
other, to form a pipeline of cells representing your algorithm. The data is forwarded through the pipeline one by
one or in batch.

.. figure:: /data/pypipeline-docs-example-tracking-1.png
   :scale: 80 %

   An example usecase for PyPipeline: multiple object tracking in videos.


On top of this structured approach to algorithm building, PyPipeline adds parallelism and upscaling for free.
Cells can be executed using pipeline parallelism, and bottleneck cells can be duplicated to increase your pipeline's
throughput. The framework handles all locking, synchronization, ordering, queuing and threadsafety for you.


.. figure:: /data/pypipeline-docs-example-tracking-2.png

   The same usecase, but with ScaleUpCells included to allow the scaling up of bottlenecks.


The idea is that cells, inputs, outputs and connections are real objects interacting with
each other. Incoming data arrives at a cell's inputs, the cell performs its operation on it, and
provides the results to its outputs when ready. The connections then route these results to the next cells.


On the roadmap
--------------

The following features are on the roadmap for development:
 * Auto-generate a full HTTP interface on top of your pipeline, allowing to serve your pipeline immediately.
   (**Currently being developed**)
 * Clone-specific configuration and resource management (GPU allocation, ...) of scale-up cells.
 * A web-based GUI for visualizing and profiling active pipelines: timing info, bottlenecks, ...
 * Push-based execution?

Other requests? Let me know!
