# tableau-server-rest-api-and-hyper-api-use-cases
This repository stores scripts I created for actual business scenarios using Tableau Server REST API and Hyper API in Python.
**Sensitive data including web address, log-in credential, and id has been deleted in the scripts.**

### Scenario 1 - Refreshing data sources on server based on tag
Users would be able to refresh data extracts on server based on the extracts' tag name. Combining this script with other applications such as Alteryx makes data source refresh on demand more efficient and accurate. 

### Scenario 2 - Incremental extract update with new data from Tableau Server repository
Postgres database for Tableau Server repository only saves [183 days](https://help.tableau.com/current/server/en-us/adminview_postgres.htm?_gl=1*1fvhps6*_ga*MTk4Mjk2OTkyMy4xNjE0Njk4Mjgw*_ga_8YLN0SNXVS*MTYyMjcyOTg4Ny45Ny4xLjE2MjI3MzAxNzkuMA..) of data by default. The script enables preservation of historical data in hyper extracts without having to change the configuration. 

Settings of scenario 2: 
1. There has been a published extract on server that refreshes daily to get latest data from Tableau server repository.
2. The connection to PostgreSQL database (Tableau server repository) is managed through the published extract and thus not required in this script.
3. A hyper extract and a .tdsx workbook (optional) containing historical data are saved in a local repository.

References for Hyper API:
1. [YouTube - Building on Tableau: Hyper API & Connected SDK | E4](https://www.youtube.com/watch?v=mlcyeo25mHQ)
2. [Tableau Hyper API Documentation](https://help.tableau.com/current/api/hyper_api/en-us/index.html)
3. [GitHub - Hyper API Samples](https://github.com/tableau/hyper-api-samples/tree/main/Tableau-Supported/Python)
