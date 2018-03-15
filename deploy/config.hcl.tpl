NOMAD_ADDR=https://192.168.85.1:4646
NOMAD_REGION={{ env "NOMAD_REGION" }}
NOMAD_NAMESPACE=default
NOMAD_CACERT=/local/ca.pem
NOMAD_CLIENT_CERT=/local/cert.pem
NOMAD_CLIENT_KEY=/local/key.pem
NOMAD_SKIP_VERIFY=false
LOG_FILE=/logs/nomad-forwarder.log

