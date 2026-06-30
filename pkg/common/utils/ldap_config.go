/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package utils

import (
	"strings"

	v1 "k8s.io/api/core/v1"
)

const (
	YunikornSecretName       = "yunikorn-secret"
	LdapPrefix               = "ldap."
	LdapSecretUsernameKey    = "ldap.username"
	LdapSecretPasswordKey    = "ldap.password"
	LdapExtraBindUserKey     = "ldap.BindUser"
	LdapExtraBindPasswordKey = "ldap.BindPassword"
)

// MergeLdapExtraConfig copies ldap.* settings from the flattened config map and injects
// bind credentials from the Kubernetes secret into the ExtraConfig map.
func MergeLdapExtraConfig(extraConfig map[string]string, flatConfig map[string]string, secret *v1.Secret) map[string]string {
	result := make(map[string]string, len(extraConfig)+10)
	for key, value := range extraConfig {
		result[key] = value
	}

	for key, value := range flatConfig {
		if strings.HasPrefix(key, LdapPrefix) {
			result[key] = value
		}
	}

	if secret != nil && secret.Data != nil {
		if username, ok := secret.Data[LdapSecretUsernameKey]; ok {
			result[LdapExtraBindUserKey] = string(username)
		}
		if password, ok := secret.Data[LdapSecretPasswordKey]; ok {
			result[LdapExtraBindPasswordKey] = string(password)
		}
	}

	return result
}
