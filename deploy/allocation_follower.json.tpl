{
    "Job": {
        "Datacenters": ["[[.dataCenter]]"],
        "ID": "log-follower-[[.environment]]",
        "Name": "log-follower-[[.environment]]",
        "Region": "[[.region]]",
        "Type": "system",
        "TaskGroups": [
            {
                "Name": "service",
                "Count": 1,
                "RestartPolicy": {
                    "Attempts": 2,
                    "Delay": 15000000000,
                    "Interval": 60000000000,
                    "Mode": "delay"
                },
                "Tasks": [
                    {
                        "Name": "log-forwarder",
                        "KillTimeout": 5000000000,
                        "LogConfig": {
                            "MaxFileSizeMB": 10,
                            "MaxFiles": 10
                        },
                        "Driver": "docker",
                        "Config": {
                            "image": "npdsoftwaredev/nomad_follower:[[.containerRef]]",
                            "network_mode": "cluster_network",
                            "volumes": ["/var/log:/logs:Z"]
                        },
                        "Resources": {
                            "CPU": 200,
                            "MemoryMB": 350,
                            "Networks": [
                                {
                                    "MBits": 100
                                }
                            ]
                        },
                        "Artifacts": [
                            {
                                "GetterSource":"[[.repoPath]]//deploy",
                                "RelativeDest":"repo",
                                "GetterOptions": {
                                    "sshkey": "[[.githubsshkey]]",
                                    "ref": "[[.repoRef]]"
                                }
                            }
                        ],
                        "Templates": [
                            {
                                "SourcePath": "repo/config.hcl.tpl",
                                "DestPath": "local/config.env",
                                "Envvars": true,
                                "ChangeMode": "restart"
                            },
                            {

                                "EmbeddedTmpl": "{{ with secret \"/secret/cluster/root/certificates\" }}{{ .Data.root.cert }}{{ end }}",
                                "DestPath": "local/ca.pem",
                                "ChangeMode": "restart"
                            },
                            {
                                "EmbeddedTmpl": "{{ with secret \"/secret/cluster/nomad/certificates\" }}{{ .Data.nomad_cli.cert }}{{ end }}",
                                "DestPath": "local/cert.pem",
                                "ChangeMode": "restart"
                            },
                            {
                                "EmbeddedTmpl": "{{ with secret \"/secret/cluster/nomad/certificates\" }}{{ .Data.nomad_cli.key }}{{ end }}",
                                "DestPath": "local/key.pem",
                                "ChangeMode": "restart"
                            }
                        ],
                        "Vault": {
                            "Policies": ["deploy_infrastructure_components"]
                        }
                    }
                ]
            }
        ]
    }
}