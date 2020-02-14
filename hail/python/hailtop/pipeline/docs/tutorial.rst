.. _sec-tutorial:

========
Tutorial
========

This tutorial goes through the basic concepts of Pipeline with examples.

Import
------

Pipeline is located inside the `hailtop` module, which can be installed
as described in the :ref:`Getting Started <sec-getting_started>` section.

.. code-block:: python

    >>> import hailtop.pipeline as hp


f-strings
---------

f-strings were added to Python in version 3.6 and are denoted by the 'f' character
before a string literal. When creating the string, Python evaluates any expressions
in single curly braces `{...}` using the current variable scope. When Python compiles
the example below, the string 'Alice' is substituted for `{name}` because `name` is set
to 'Alice'.

.. code-block:: python

    >>> name = 'Alice'
    >>> print(f'hello {name}')
    'hello Alice'

You can put any arbitrary Python code inside the curly braces and Python will evaluate
the expression correctly. For example, below we evaluate `x + 1` first before compiling
the string. Therefore, we get 'x = 6' as the resulting string.

.. code-block:: python

    >>> x = 5
    >>> print(f'x = {x + 1}')
    'x = 6'

To use an f-string and output a single curly brace in the output string, escape the curly
brace by duplicating the character. For example, `{` becomes `{{` in the string definition,
but will print as `{`. Likewise, `}` becomes `}}`, but will print as `}`.

.. code-block:: python

    >>> x = 5
    >>> print(f'x = {{x + 1}} plus {x}')
    'x = {{x + 1}} plus 5'

To learn more about f-strings, check out this `tutorial <https://www.datacamp.com/community/tutorials/f-string-formatting-in-python>`_.

Hello World
-----------

We start by defining what is a Pipeline.


Single Task
~~~~~~~~~~~

.. code-block:: python

    >>> p = hp.Pipeline(name='hello-single')
    >>> t = p.new_task(name='t1')
    >>> t.command('echo "hello world"')
    >>> p.run()


Parallel Tasks
~~~~~~~~~~~~~~

.. code-block:: python

    >>> p = hp.Pipeline(name='hello-parallel')
    >>> s = p.new_task(name='t1')
    >>> s.command('echo "hello world 1"')
    >>> t = p.new_task(name='t2')
    >>> t.command('echo "hello world 2"')
    >>> p.run()


Dependent Tasks
~~~~~~~~~~~~~~~

.. code-block:: python

    >>> p = hp.Pipeline(name='hello-serial')
    >>> s = p.new_task(name='t1')
    >>> s.command('echo "hello world 1"')
    >>> t = p.new_task(name='t2')
    >>> t.command('echo "hello world 2"')
    >>> t.depends_on(s)
    >>> p.run()


.. code-block:: python

    >>> p = hp.Pipeline(name='hello-serial')
    >>> s = p.new_task(name='t1')
    >>> s.command(f'echo "hello world" > {s.ofile}')
    >>> t = p.new_task(name='t2')
    >>> t.command('cat {s.ofile}')
    >>> p.run()


Scatter / Gather
----------------

.. code-block:: python

    >>> p = hp.Pipeline(name='scatter')
    >>> for name in ['Alice', 'Bob', 'Dan']:
    ...     t = p.new_task(name=name)
    ...     t.command(f'echo "hello {name}"')
    >>> p.run()


.. code-block:: python

    >>> p = hp.Pipeline(name='scatter-gather-1')
    >>> tasks = []
    >>> for name in ['Alice', 'Bob', 'Dan']:
    ...     t = p.new_task(name=name)
    ...     t.command(f'echo "hello {name}"')
    ...     tasks.append(t)
    >>> sink = p.new_task(name='sink')
    >>> sink.depends_on(*tasks)
    >>> p.run()


.. code-block:: python

    >>> p = hp.Pipeline(name='scatter-gather-2')
    >>> tasks = []
    >>> for name in ['Alice', 'Bob', 'Dan']:
    ...     t = p.new_task(name=name)
    ...     t.command(f'echo "hello {name}" > {t.ofile}')
    ...     tasks.append(t)
    >>> sink = p.new_task(name='sink')
    >>> sink.command('cat {}'.format(' '.join([t.ofile for t in tasks]))
    >>> p.run()


Nested Scatters
---------------

.. code-block:: python

    >>> def do_chores(p, user):
    ...     make_bed = p.new_task(name=f'{user}-make-bed',
    ...                           attributes={'user': user})
    ...     laundry = p.new_task(name=f'{user}-laundry',
    ...                          attributes={'user': user})
    ...     grocery_shop = p.new_task(name=f'{user}-grocery-shop',
    ...                               attributes={'user': user})
    ...     grocery_shop.depends_on(make_bed, laundry)
    ...     return grocery_shop

    >>> p = hp.Pipeline(name='nested-scatter')
    >>> user_chores = [do_chores(p, user)
    ...                for user in ['Alice', 'Bob', 'Dan']]
    >>> all_done = p.new_task(name='sink')
    >>> all_done.depends_on(*user_chores)
    >>> p.run()


Input Files
-----------

.. code-block:: python

    >>> p = hp.Pipeline(name='hello-input')
    >>> input = p.read_input('data/hello.txt')
    >>> t = p.new_task(name='hello')
    >>> t.command('cat {input}')
    >>> p.run()


Output Files
------------

.. code-block:: python

    >>> p = hp.Pipeline(name='hello-input')
    >>> t = p.new_task(name='hello')
    >>> t.command('echo "hello" > {t.ofile}')
    >>> p.write_output(t.ofile, 'output/hello.txt')
    >>> p.run()


Resource Groups
---------------

.. code-block:: python

    >>> p = hp.Pipeline(name='resource-groups')
    >>> bfile = p.read_input_group(bed='data/example.bed',
    ...                            bim='data/example.bim',
    ...                            fam='data/example.fam')
    >>> wc_bim = p.new_task(name='wc-bim')
    >>> wc_bim.command(f'wc -l {bfile.bim}')
    >>> wc_fam = p.new_task(name='wc-fam')
    >>> wc_fam.command(f'wc -l {bfile.fam}')
    >>> p.run()


.. code-block:: python

    >>> p = hp.Pipeline(name='resource-groups')
    >>> create = p.new_task(name='create-dummy')
    >>> create.declare_resource_group(bfile={'bed': '{root}.bed',
    ...                                      'bim': '{root}.bim',
    ...                                      'fam': '{root}.fam'}
    >>> create.command(f'plink --dummy 10 100 --make-bed --out {create.bfile}')
    >>> p.run()
