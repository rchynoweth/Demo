my_secret = dbutils.secrets.get("rac_scope", "oneenvtenantid")

for s in my_secret:
  print(s)