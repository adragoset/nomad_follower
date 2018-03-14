NOMAD_ADDR=192.168.85.1:4646
NOMAD_REGION={{ env "NOMAD_REGION" }}
NOMAD_NAMESPACE=default
NOMAD_CACERT=/certs/cluster-ca.pem
NOMAD_CLIENT_CERT=/certs/nomad-client.pem
NOMAD_CLIENT_KEY=/certs/nomad-client-key.pem
NOMAD_SKIP_VERIFY=false
NOMAD_TOKEN=
LOG_FILE=/logs/nomad-forwarder.log

