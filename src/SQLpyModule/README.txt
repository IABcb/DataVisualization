
#################### LINKING PYTHON WITH PostgreSQL #####################
Author: Raúl Sánchez Martín
Date: 28/09/2016
--------------------------------------------------------------------------

In this module, a simple function to link Python with PostgreSQL is provied

###-------------------------Prerequisites-----------------------------###

1) 	A PostgreSQL server should be running on port 5432.
2) 	The python library "psycopg2" should be installed. In order to install 
   	this library, you should introduce in the command line the following
   	command: "sudo apt-get install python-psycopg2". This will install 
	the "psycopg2" package in your root folder.

###-------------------------Installation------------------------------###

1)	Set the working directory to the module folder location
2)  Type the following command: sudo pip install .
3)	The previous command will install this module in the root directory.
	If you want to install it in a virtual enviroment, you should also 
	install the "psycopg2" in that virtual enviroment


###---------------------------Examples--------------------------------###

You can find some examples on the "exampleSQL.py" file.

