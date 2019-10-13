package main

import (
	"time"

	nomadApi "github.com/hashicorp/nomad/api"
	vaultApi "github.com/hashicorp/vault/api"
)

// NomadConfig is a common interface to use a Nomad client + optionally renew tokens.
type NomadConfig interface {
	Client() *nomadApi.Client
	RenewToken()
}

// NomadEnvAuth uses the standard $NOMAD_TOKEN to auth to Nomad, if ACLs are enabled.
type NomadEnvAuth struct {
	client *nomadApi.Client
	config *nomadApi.Config
	log Logger
}

// Client returns a pointer to the internal Nomad client struct.
func (n *NomadEnvAuth) Client() *nomadApi.Client {
	return n.client
}

// RenewToken is a no-operation call to meet the NomadConfig interface.
func (n *NomadEnvAuth) RenewToken() {
	// nothing to do $NOMAD_TOKEN is used or cluster does not use ACLs
}

// NewNomadEnvAuth returns a new standard-auth config relying on $NOMAD_TOKEN.
func NewNomadEnvAuth(nomadConfig *nomadApi.Config, logger Logger) *NomadEnvAuth {
	var n = NomadEnvAuth{}
	var err error
	if nomadConfig == nil {
		n.config = nomadApi.DefaultConfig()
	} else {
		n.config = nomadConfig
	}
	n.client, err = nomadApi.NewClient(n.config)
	if err != nil {
		logger.Error("NewNomadEnvAuth", "Failed to create Nomad client.")
		return nil
	}
	n.log = logger
	return &n
}

// NomadRenewableAuth uses a Vault Nomad backend to auth to Nomad and can renew tokens.
type NomadRenewableAuth struct {
	client *nomadApi.Client
	config *nomadApi.Config
	log Logger

	vaultClient *vaultApi.Client
	vaultConfig *vaultApi.Config

	nomadTokenBackend string
	circuitBreak time.Duration
	lastRenewTime time.Time
}

// Client returns a pointer to the internal Nomad client struct.
func (n *NomadRenewableAuth) Client() *nomadApi.Client {
	return n.client
}

// RenewToken uses a Vault Nomad secrets backend to create a new Nomad token.
func (n *NomadRenewableAuth) RenewToken() {
	logContext := "NomadRenewableAuth.RenewToken"
	// check if circuit breaker tripped on token renewal
	t := time.Now()
	past := t.Sub(n.lastRenewTime)
	if past < n.circuitBreak {
		n.log.Trace(logContext, "Circuit breaker already tripped, skipping request")
		return
	}
	vaultReader := n.vaultClient.Logical()
	resp, err := vaultReader.Read(n.nomadTokenBackend)
	if err != nil {
		n.log.Errorf(logContext, "Error reading Nomad token backend: %s", err)
		return
	}

	if resp == nil || resp.Data == nil || len(resp.Data) == 0 {
		n.log.Error(logContext, "Error no secret data at path")
		return
	}

	// pull out nomad token
	token, ok := resp.Data["secret_id"].(string)
	if !ok {
		n.log.Error(logContext, "Nomad token not string in Vault response")
		return
	}

	// set in client
	n.log.Debug(logContext, "Refreshed Nomad token from Vault")
	n.client.SetSecretID(token)
	n.lastRenewTime = time.Now()
}

// NewNomadRenewableAuth returns a config reliant on a Vault Nomad backend to provide auth tokens.
func NewNomadRenewableAuth(nomadConfig *nomadApi.Config, vaultConfig *vaultApi.Config, tokenBackend string, circuitBreak time.Duration, logger Logger) *NomadRenewableAuth {
	var n = NomadRenewableAuth{}
	var err error
	logContext := "NewNomadRenewableAuth"
	if nomadConfig == nil {
		n.config = nomadApi.DefaultConfig()
	} else {
		n.config = nomadConfig
	}
	if vaultConfig == nil {
		n.vaultConfig = vaultApi.DefaultConfig()
		n.vaultConfig.ReadEnvironment()
	} else {
		n.vaultConfig = vaultConfig
	}
	n.vaultClient, err = vaultApi.NewClient(n.vaultConfig)
	if err != nil {
		logger.Error(logContext, "Failed to create Vault client")
		return nil
	}
	n.client, err = nomadApi.NewClient(n.config)
	if err != nil {
		logger.Error(logContext, "Failed to create Nomad client")
		return nil
	}
	n.nomadTokenBackend = tokenBackend
	n.circuitBreak = circuitBreak
	n.log = logger
	return &n
}
