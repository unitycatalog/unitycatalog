package uc.authz

import rego.v1

default allow := false

# The local bootstrap account needs an OPA policy too.
allow if input.principal.email == "admin"

allow if {
	some grant in data.grants
	grant.principal == input.principal.email
	grant.privilege == input.privilege
	grant.type == input.resource.type
	grant.id == input.resource.id
}
