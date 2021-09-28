# NADC_PGLOAD: Load NADC Data to Postgres
**NADC** = Nebraska Accountability and Disclosure Commission

Use Git to clone the repository from GitHub to install the latest version:

    git clone https://github.com/timmysql/NADC_PGLOAD.git
    cd nadc_pgload
    pip install .

Alternatively, install directly from the GitHub repository:

pip install git+https://github.com/tweepy/tweepy.git

Use virtualenv (if you're cool):

    pip install virtualenv
    cd (/location/of/your/nadc_pgload)
    virtualenv venv

Initialize your venv:

    source /venv/bin/activate

Install requirements.txt

    pip install requirements.txt


Install Postgresql and create an nadc database.
      
https://www.digitalocean.com/community/tutorials/how-to-install-postgresql-on-ubuntu-20-04-quickstart
