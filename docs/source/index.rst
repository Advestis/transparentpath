.. transparentpath documentation master file, created by
   sphinx-quickstart on Mon Oct 31 10:30:53 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

==============================
transparentpath documentation
==============================

A class that allows one to use a path in a local file system or a Google Cloud Storage (GCS) file system or SFTP file system in the same way one would use a pathlib.Path object.
One can use many different GCP projects at once.


Contents
---------
.. toctree::
   :maxdepth: 1

   installation
   readme

.. toctree::
   :maxdepth: 1
   :hidden:

   gcsutils
   io

.. grid:: 2

    .. grid-item::

        .. card:: Gcsutils
            :text-align: center


            .. button-ref:: gcsutils
                :color: primary
                :shadow:

                gcsutils

    .. grid-item::

        .. card:: IO
            :text-align: center


            .. button-link:: io.html
                :color: primary
                :shadow:

                IO