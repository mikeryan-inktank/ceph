==========================
 Installing Ceph Manually
==========================
Ceph is intended for large-scale deployments, but you may install Ceph on a
single host. This guide is intended for Debian/Ubuntu Linux distributions.

#. `Install Ceph packages`_
#. Create a ``ceph.conf`` file. 
   See `Ceph Configuration Files`_ for details.
#. Deploy the Ceph configuration.	
   See `Deploy with mkcephfs`_ for details.
#. Start a Ceph cluster.
   See `Starting a Cluster`_ for details.
#. Mount Ceph FS. 
   See `Ceph FS`_ for details.


.. _Install Ceph packages: ../../install/debian
.. _Ceph Configuration Files: ../../config-cluster/ceph-conf
.. _Deploy with mkcephfs: ../../config-cluster/mkcephfs
.. _Starting a Cluster: ../../init/start-cluster/
.. _Ceph FS: ../../cephfs/