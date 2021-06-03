# tableau-server-rest-api-and-hyper-api-use-cases
This repository stores uses cases of Tableau Server REST API and Hyper API in actual business scenarios.
**Sensitive data including web address, log-in credential, and ids is deleted in the scripts.**

### Scenario 1 - Refreshing data sources on server based on Tag
Users would be able to refresh data extracts on server based on the extracts' tag name. Combining this script with other application such as Alteryx makes data source refresh on demand more efficient and accurate. 

### Scenario 2 - Incremental update with new data from Tableau Server repository
Postgres database for Tableau Server repository only saves [183 days](https://help.tableau.com/current/server/en-us/adminview_postgres.htm?_gl=1*1fvhps6*_ga*MTk4Mjk2OTkyMy4xNjE0Njk4Mjgw*_ga_8YLN0SNXVS*MTYyMjcyOTg4Ny45Ny4xLjE2MjI3MzAxNzkuMA..) of data by default. The script enables preservation of historical data without having to change the configuration. 

Setting of scenario 2: 
1. There has been a published extract on server that refreshes daily to get latest data from Tableau server repository.
2. A hyper extract and a .tdsx workbook (optional) containing historical data are saved in the local repository.
