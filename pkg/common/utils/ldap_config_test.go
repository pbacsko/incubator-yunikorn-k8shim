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
	"testing"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestMergeLdapExtraConfig(t *testing.T) {
	extra := map[string]string{
		"log.level": "INFO",
	}
	flat := map[string]string{
		"ldap.Host":   "ldap.example.com",
		"ldap.Port":   "389",
		"queues.yaml": "ignored",
	}
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: YunikornSecretName},
		Data: map[string][]byte{
			LdapSecretUsernameKey: []byte("cn=admin,dc=example,dc=com"),
			LdapSecretPasswordKey: []byte("secret"),
		},
	}

	result := MergeLdapExtraConfig(extra, flat, secret)

	assert.Equal(t, "INFO", result["log.level"])
	assert.Equal(t, "ldap.example.com", result["ldap.Host"])
	assert.Equal(t, "389", result["ldap.Port"])
	assert.Equal(t, "cn=admin,dc=example,dc=com", result[LdapExtraBindUserKey])
	assert.Equal(t, "secret", result[LdapExtraBindPasswordKey])
	_, ok := result["queues.yaml"]
	assert.Equal(t, false, ok)
}

func TestMergeLdapExtraConfigMissingSecret(t *testing.T) {
	flat := map[string]string{
		"ldap.Host": "ldap.example.com",
	}
	result := MergeLdapExtraConfig(nil, flat, nil)
	assert.Equal(t, "ldap.example.com", result["ldap.Host"])
	_, ok := result[LdapExtraBindUserKey]
	assert.Equal(t, false, ok)
}
