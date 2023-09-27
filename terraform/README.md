# Initialize state file (.tfstate)
```sh
terraform init
```

# Check changes to new infra plan
```sh
terraform plan -var="project=dtc-de-course-388001"
```

```shell
# Create new infra
terraform apply -var="project=dtc-de-course-388001"
```

```shell
# Delete infra after your work, to avoid costs on any running services
terraform destroy
```
