# Welcome to the Propel Holdings Enterprise Team DBT project  #

The Enterprise team is responsible for {TBD}. 

Data can be consumed directly from Snowflake by power users (via the Snowflake UI, python, etc.) or via BI reporting tools like Domo. 

Data from the following sources have been ingested so far:

- {TBD}
- {TBD}
- {TBD}
- etc.

Most of the data is automatically ingested using {TBD} (more information in this [repo]({TBD})).

## Repository Structure ##

```bash
├── macros/                 # Custom Jinja macros
├── models/
│   ├── landing/            # Includes a landing.yml detailing where to get the source data from. 
│   ├── prepare/       
│       ├── /{sources}/     # sources ∈ {tbd, ...}.
│   ├── normalize/ 
│       ├── /{sources}/      
|   ├── integrate/     
│       ├── /dimensions/    # Contains the .sql files for the dimensions data objects of the star schema.
│       ├── /facts/         # Contains the .sql files for the facts data objects of the star schema.
│   └── marts/    
│       ├── /dimensions/    
│       ├── /facts/    
│       ├── /dataproducts/  # Contains the dataproducts (agg tables {TBD}?) table used as the basis for reports created in the /visuals/ folder   
│       ├── /visuals/       # Domo reports      
├── tbd_pipeline files.yml  # Contains the  Github pipeline specs --- see the "The deployment pipeline (CICD)" section below.
├── dbt_project.yml         # dbt project configuration
├── packages.yml            # Contains the packages that need to be installed --- see the "Using the dbt-utils package to generate surrogate keys" section below.
├── profiles.yml            # --- see the "dbt profiles" section below.
├── requirements.txt        # Contains the dbt version to install --- see the "Installing dbt" section below.
└── README.md               # Documentation
```

**Note**: There are other folders that are supported in a dbt project structure like analysis, docs, seeds, snapshot, tests, etc. 
These are not included above as we are not using them so far but they could be used in the future (additional information [here](https://docs.getdbt.com/docs/build/projects)).


## Data models: ##

There are 5 layers in the data model (see  `/models` folder):

1. LANDING: Where data is written in Snowflake (by Snowpipes, Openflow, etc.)
2. PREPARE: Where (if needed) we transform/flatten the raw data (often .json)
in a workable table structure.
3. NORMALIZE: This is where the data is cleaned and standardized to an
agreed-upon format (for dates, numbers, currencies, locations,
addresses, etc.). 
This is also where data is **deduplicated**. 
4. INTEGRATE: Where we build the facts and dimension tables of the star
schema.
5. MARTS: The final data products for consumption. This includes
value-added (dataproducts & reporting) tables developed for reporting (+ other needs) 
and the finalized dimensions and facts tables of the star schema for the power-users of the business.

6. GOVERNANCE: {TBD} Maybe SECURITY too.

────────────

Here's the data flow between the layers:

| Data Stage  | Next Step |
|------------|-------------|
| 🔴 LANDING  | 🟡 PREPARE  |
| 🟡 PREPARE  | 🟠 NORMALIZE  |
| 🟠 NORMALIZE  | 🔵 INTEGRATE, 🟣 GOVERNANCE  |
| 🔵 INTEGRATE  | 🟢 MARTS  |



---

## Getting Started ##

### Installing dbt ###

Make sure [python is installed](https://www.python.org/downloads/windows/) and that **pip and python** are added to your PATH environment variables. \

**Create a python environment** as this will allow us to isolate all the required dependencies from whatever is already set up on your computer.

Run `python -m venv venv` to create the venv virtual environment.

To activate the virtual environment run `venv\Scripts\activate` 

Within the virtual environment, install dbt by running  `pip install -r requirements.txt` 

dbt is now installed cally.

### Connect dbt to Snowflake ###

Modify the environment variables in the file that activates your virtual environment (Windows: `.\venv\Scripts\Activate.bat`)
with the Snowflake credentials with which you want dbt to access Snowflake. 

```batch
set DBT_SNOWFLAKE_ACCOUNT=$$$$$$$$$$.ca-central-1.aws
set DBT_SNOWFLAKE_USER=$$$$$$$$$$
set DBT_SNOWFLAKE_ROLE=$$$$$$$$$$
set DBT_SNOWFLAKE_PREFIX_DATABASE=$$$$$$$$$$
set DBT_SNOWFLAKE_WAREHOUSE=$$$$$$$$$$
set DBT_SNOWFLAKE_SCHEMA=$$$$$$$$$$
```

After that is done, activate your virtual environment ( `venv\Scripts\activate` ) <br>
and run `dbt debug --target local`

You should see the following output:

```batch
Connection test: [✅ OK connection ok]
✅ All checks passed!
```

### dbt profiles ###

dbt profiles are defined in the [profiles.yml](profiles.yml) file, which contains the credentials and settings dbt uses to connect to Snowflake. <br> More information about each profile to come.

### Common dbt commands ###

```bash
dbt debug  # Test the connection
dbt deps   # Install dependencies
dbt seed   # Load seed files
dbt run    # Run transformations.
dbt test   # Run data tests
dbt docs generate && dbt docs serve  # Generate and view documentation
```
Additional documentation [here](https://docs.getdbt.com/reference/dbt-commands)

**Note1**: you can run specifc dbt models using the --select flag.<br>
**Note2**: you can run specific target profiles using the --target  flag.


## Best practices & syntax ##

- **Use snake_case for column names** :

    <br> By default, Snowflake  will save column names in UPPERCASE. This means a column named `AS BusinessApplicationCode` in Snowflake will be saved as BUSINESSAPPLICATIONCODE. Saving the column using snake_case with `AS Business_Application_Code` will make BUSINESS_APPLICATION_CODE less ambiguous.


- **Naming .sql files**: 
<br><br> For the 🟡 prepare and 🟠 normalize layers, the naming convention should be {*layer_abbreviation*}\_{*source_abbreviation*}_{*data_object_name_snake_case*}.sql
<br> So *layer_abbreviation* ∈ {prep, norm}, (so far) *source_abbreviation* ∈ {jaglms, insruance, finance} 

    <br> For the 🔵 integrate and 🟢 marts layers, the naming convention should be {*layer_abbreviation*}\_{*data_category_abbreviation*}_{*data_object_name_snake_case*}.sql
<br> So *layer_abbreviation* ∈ {int, mart} & *data_category_abbreviation* ∈ {dim, fact, dataproduct, viz} 

- **Folder structure for the dbt model layers**: 

    See the [Repository Structure](#Repository Structure) section. It is by source (ex: jaglms, insruance, finance, etc.) for the 🟡 prepare and 🟠 normalize layers. It is by data_category (ex: dimensions, facts, dataproducts, visuals) for the 🔵 integrate and 🟢 marts layers.

- **SQL syntax styling**: 

    <br>TBD & then implemented across all models - this [link](https://docs.getdbt.com/best-practices/how-we-style/2-how-we-style-our-sql) could be of interest.<br><br>


More to come

## The deployment pipeline (CICD) ##

{TBD}

<br>

## What moves the data from layer to layer - (Dynamic tables and orchestration ##


{TBD}


## Additional Resources ##

- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

