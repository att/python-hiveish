Why does this folder exist
=======

Hadoopy expects *executable* scripts to be passed to it's launch command. These are full mapreduce jobs packaged as executable scripts. 

How to utilize the shebang magic
=======
NOTE THAT Hadoopy does NOT activate any virtual environments itself. This caused me problems because all of my 3rd party
Python packages are installed in a virtual environment accessible on the cluster (`/home/..`). Instead of hardcoding that library
into the shebangs on these files (to keep this library generic), I've put the generic `#!/usr/bin/env python` on them. To make this work, 
in the Hadoopy launch command, send in 

        args['cmdenvs'] = ['export VIRTUAL_ENV=path_to_your_venv/','export PYTHONPATH=path_to_your_venv/', 'export PATH=path_to_your_venv//bin:$PATH'    ]
    
This sets up $VIRTUAL_ENV and $PATH to point to your virtualenv with all of your packages installed, which then properly utilizes the magical 
`#!/usr/bin/env python` shebangs. 