# Visual Studio Code Extension Built by Databricks 

Resources:
- [Extension](https://marketplace.visualstudio.com/items?itemName=databricks.databricks)  
- [Documentation](https://docs.databricks.com/dev-tools/vscode-ext.html#advanced-tasks)  

Example Console Output:  
![](/VSCode/docs/Terminal.png)

Tips:
- If you have an existing repo that you are making your Sync Destination then make sure that you pull all changes inside of Databricks. Otherwise, it may have a conflict of you attempting to commit a file that has already existed. 
- The git operations that happen locally do not take place in Databricks. So if I push changes from VS Code then I will need to pull them in Databricks. 
- If you want to import notebook default libraries you can use the following imports in python files. 
    - `from databricks.sdk.runtime import *` (Runtime 12+)
    - `from databricks.sdk.runtime import dbutils` (Runtime 12+)
    - from pyspark.dbutils import DBUtils # runtime < 12
