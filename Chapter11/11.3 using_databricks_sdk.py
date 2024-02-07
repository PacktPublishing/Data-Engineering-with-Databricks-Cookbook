from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# List the existing clusters
clusters = w.clusters.list()

# Loop through the clusters and print their names and states
for cluster in clusters:
    print(f"Cluster name: {cluster.cluster_name} - {cluster.state}")