.. _sec-batch_service:

=============
Batch Service
=============


What is the Batch Service?
--------------------------

Instead of executing jobs on your local computer (the default in Pipeline), you can execute
your jobs on a multi-tenant compute cluster in Google Cloud that is managed by the Hail team
and is called Batch. Batch consists of a scheduler that receives job submission requests
from users and then executes jobs in Docker containers on Google Compute Engine VMs that are shared amongst
all Batch users. A UI is available at `batch.hail.is` that allows a user to see job progress and access logs.


Billing
-------

The cost for executing a job depends on the underlying machine type and how much CPU and
memory is being requested. The costs are as follows:

- Compute cost
- 


Setup
-----

We assume you've already installed Pipeline as described in the
:ref:`Getting Started <sec-getting_started>` section and we have
created a user account for you and given you a billing project.

.. code-block:: sh

    hailctl auth login


include screen shots of logging in with a google account

Default is to use the :class:`.LocalBackend`. Specify the :class:`.BatchBackend`
with a valid billing project.

.. code-block:: python

    >>> import hailtop.pipeline as hp
    >>> backend = hp.BatchBackend('test') # replace 'test' with your own billing project
    >>> p = hp.Pipeline(backend=backend, name='test')
    >>> t = p.new_task(name='hello')
    >>> t.command('echo "hello world"')
    >>> p.run()


Using the UI
------------

Go to `batch.hail.is/batches`

Screen shot of batches page with batch 1
Note cost column, name comes from the name argument to Pipeline object


Click on the jobs page
Note cost column, name comes from the name argument to the Task Object

Click on an individual job page
Shows the logs for each of the containers (input, main, output)
Status includes the job spec, and statistics about each container run and an error message if it exists
