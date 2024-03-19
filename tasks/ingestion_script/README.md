## Development task
### Conda virtual environment manager is used 
List all env 
`conda env list`
List all package in current env
`conda list`
Delete env
`conda env remove <env-name>`
Create new env from Conda base env.
`conda create -n <env_name> [packages]`
Active a specific env for workspace
`conda activate <env_name>`
Return to base env 
`conda deactive `
Install packages to existing env, first active that env then run:
`conda install <package>`
The api of open-meteo need openmeteo-requests, requests-cache retry-requests, numpy, pandas

### Connection parameter
In order to connect to psql, a database.ini is required. Create once in this folder and parse your connection infomation like below:
```
[postgresql]
host=localhost
database=suppliers
user=YourUsername
password=YourPassword
port=DatabasePort
```

