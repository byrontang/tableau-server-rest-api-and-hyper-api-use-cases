import pandas as pd
import tableauserverclient as TSC

"""Sign In"""
tag = 'test'
account = 'account'
password = 'password'
server = 'server'
tableau_auth = TSC.TableauAuth(account, password)
# add user_server_version=True to use the latest version
server = TSC.Server(server, use_server_version=True)
print(server.version)

"""Get Site Info"""
with server.auth.sign_in(tableau_auth):
    all_sites, pagination_item = server.sites.get()
    df_site = pd.DataFrame({
        'name': [site.name for site in all_sites
                 if site.state == 'Active' and site.content_url != ''],
        'content_url': [site.content_url for site in all_sites
                        if site.state == 'Active' and site.content_url != '']
    })

print(df_site)

"""Get Data Source by Site"""
df_datasource = pd.DataFrame({'site':[],
                              'content_url':[],
                              'project':[],
                              'data_source':[],
                              'id':[],
                              'tag':[]})

for i in range(len(df_site)):
    tableau_auth = TSC.TableauAuth(account, password, df_site['content_url'][i])
    with server.auth.sign_in(tableau_auth):
            all_datasources, pagination_item = server.datasources.get()
            df_datasource = df_datasource.append(pd.DataFrame({
                'site': df_site['name'][i],
                'content_url': df_site['content_url'][i],
                'project': [datasource.project_name for datasource in all_datasources],
                'data_source': [datasource.name for datasource in all_datasources],
                'id': [datasource.id for datasource in all_datasources],
                # convert set to string
                'tag': [str(datasource.tags) for datasource in all_datasources]
            }))

print(df_datasource)

"""Data Source to Refresh"""
tag_filter = df_datasource['tag'].str.contains(tag)
df_refresh = df_datasource[tag_filter]
df_refresh.reset_index(inplace=True)
print(df_refresh)

print('The following extracts are detected:')
for i in range(len(df_refresh)):
    print(df_refresh['site'][i], '-', df_refresh['project'][i], ':', df_refresh['data_source'][i])

"""Refresh Data Sources"""
for i in range(len(df_refresh)):
    tableau_auth = TSC.TableauAuth(account, password, df_refresh['content_url'][i])
    with server.auth.sign_in(tableau_auth):
        # get the data source item to update
        test_datasource = server.datasources.get_by_id(df_refresh['id'][i])

        # call the refresh method with the data source item
        try:
            refreshed_datasource = server.datasources.refresh(test_datasource)
            print('Refreshing:', df_refresh['site'][i], '-',
                  df_refresh['project'][i], ':',
                  df_refresh['data_source'][i])
        except:
            print('Manual refresh failed:', df_refresh['site'][i], '-',
                  df_refresh['project'][i], ':',
                  df_refresh['data_source'][i])