equity-procurement-analysis
==============================

Following the City’s 2021 migration of procurement data into a Salesforce database, we assessed the City’s social equitability in choosing business partners--the first attempt to do so in over 20 years!

Important Reminders
------------
1. Changing the Google Sheet's column names and tab names has profound impact on Tableau's ability to pull the data. If any changes get made to the script/google sheet, you need to check on the Tableau and make sure it is okay.
2. Changing the Google Sheet's tab orders has profound impact on the way these scripts writes to the Google Sheet. Do not ever change the tab orders. If you want to add a new tab in the Sheet, append it to the end.

Project Organization
------------

    ├── LICENSE
    ├── Makefile                 <- Makefile with commands like `make data` or `make train`
    ├── README.md                <- The top-level README for developers using this project.
    ├── data                     <- A directory for local data.
    ├── models                   <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks                <- Jupyter notebooks.
    │
    ├── references               <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports                  <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures              <- Generated graphics and figures to be used in reporting
    │
    │
    ├── conda-requirements.txt   <- The requirements file for conda installs.
    ├── requirements.txt         <- The requirements file for reproducing the analysis environment, e.g.
    │                               generated with `pip freeze > requirements.txt`
    │
    ├── setup.py                 <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                      <- Source code for use in this project.
    │   ├── __init__.py          <- Makes src a Python module
    │   │
    │   ├── data                 <- Scripts to download or generate data
    │   ├── features             <- Scripts to turn raw data into features for modeling
    │   ├── models               <- Scripts to train models and then use trained models to make
    │   └── visualization        <- Scripts to create exploratory and results oriented visualizations


--------

### Starting with JupyterLab

1. Sign in with credentials. [More details on getting started here.](https://cityoflosangeles.github.io/best-practices/getting-started-github.html) 
2. Launch a new terminal and clone repository: `git clone https://github.com/CityOfLosAngeles/REPO-NAME.git`
3. Change into directory: `cd REPO-NAME`
4. Make a new branch and start on a new task: `git checkout -b new-branch`


## Starting with Docker

1. Start with Steps 1-2 above
2. Build Docker container: `docker-compose.exe build`
3. Start Docker container `docker-compose.exe up`
4. Open Jupyter Lab notebook by typing `localhost:8888/lab/` in the browser.

### Setting up a Conda Environment 

1. `conda create --name my_project_name` 

2. `source activate my_project_name`
3. `conda install --file conda-requirements.txt -c conda-forge` 
4. `pip install requirements.txt`

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
