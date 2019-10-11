package main

import (
	"fmt"
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
	// cannot use ErrCh during setup or deadlock
	errCh chan string
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
func NewNomadEnvAuth(errCh chan string, nomadConfig *nomadApi.Config) *NomadEnvAuth {
	var n = NomadEnvAuth{}
	var err error
	if nomadConfig == nil {
		n.config = nomadApi.DefaultConfig()
	} else {
		n.config = nomadConfig
	}
	n.client, err = nomadApi.NewClient(n.config)
	if err != nil {
		//TODO make json
		fmt.Println("Failed to create Nomad client.")
		return nil
	}
	n.errCh = errCh
	return &n
}

// NomadRenewableAuth uses a Vault Nomad backend to auth to Nomad and can renew tokens.
type NomadRenewableAuth struct {
	client *nomadApi.Client
	config *nomadApi.Config
	errCh chan string

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
	// check if circuit breaker tripped on token renewal
	t := time.Now()
	past := t.Sub(n.lastRenewTime)
	if past < n.circuitBreak {
		return
	}
	vaultReader := n.vaultClient.Logical()
	resp, err := vaultReader.Read(n.nomadTokenBackend)
	if err != nil {
		n.errCh <- fmt.Sprintf("Error reading Nomad token backend: %s", err)
		return
	}

	if resp == nil || resp.Data == nil || len(resp.Data) == 0 {
		n.errCh <- fmt.Sprintf("Error no secret data at path.")
		return
	}

	// pull out nomad token
	token, ok := resp.Data["secret_id"].(string)
	if !ok {
		n.errCh <- fmt.Sprintf("Nomad token not string in Vault resp.")
		return
	}

	// set in client
	n.errCh <- fmt.Sprintf("Refreshed Nomad token from Vault.")
	n.client.SetSecretID(token)
	n.lastRenewTime = time.Now()
}

// NewNomadRenewableAuth returns a config reliant on a Vault Nomad backend to provide auth tokens.
func NewNomadRenewableAuth(tokenBackend string, errCh chan string, circuitBreak time.Duration, nomadConfig *nomadApi.Config, vaultConfig *vaultApi.Config) *NomadRenewableAuth {
	var n = NomadRenewableAuth{}
	var err error
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
		//TODO make json
		fmt.Println("Failed to create Vault client.")
		return nil
	}
	n.client, err = nomadApi.NewClient(n.config)
	if err != nil {
		//TODO make json
		fmt.Println("Failed to create Nomad client.")
		return nil
	}
	n.nomadTokenBackend = tokenBackend
	n.errCh = errCh
	n.circuitBreak = circuitBreak
	return &n
}
