/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.rest.security;

import java.util.HashSet;
import java.util.Set;

import org.springframework.ldap.core.ContextSource;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.ldap.userdetails.DefaultLdapAuthoritiesPopulator;

import com.kylinolap.rest.constant.Constant;

/**
 * @author xduo
 * 
 */
public class AuthoritiesPopulator extends DefaultLdapAuthoritiesPopulator {

    String adminRole;
    SimpleGrantedAuthority adminRoleAsAuthority;
    
    SimpleGrantedAuthority adminAuthority = new SimpleGrantedAuthority(Constant.ROLE_ADMIN);
    SimpleGrantedAuthority modelerAuthority = new SimpleGrantedAuthority(Constant.ROLE_MODELER);
    SimpleGrantedAuthority analystAuthority = new SimpleGrantedAuthority(Constant.ROLE_ANALYST);
    
    Set<GrantedAuthority> defaultAuthorities = new HashSet<GrantedAuthority>();
    
    /**
     * @param contextSource
     * @param groupSearchBase
     */
    public AuthoritiesPopulator(ContextSource contextSource, String groupSearchBase, String adminRole, String defaultRole) {
        super(contextSource, groupSearchBase);
        this.adminRole = adminRole;
        this.adminRoleAsAuthority = new SimpleGrantedAuthority(adminRole);
        
        if (defaultRole.contains(Constant.ROLE_MODELER))
            this.defaultAuthorities.add(modelerAuthority);
        if (defaultRole.contains(Constant.ROLE_ANALYST))
            this.defaultAuthorities.add(analystAuthority);
        if (defaultRole.contains(Constant.ROLE_ADMIN)) {
            this.defaultAuthorities.add(adminAuthority);
        }
    }

    @Override
    public Set<GrantedAuthority> getGroupMembershipRoles(String userDn, String username) {
        Set<GrantedAuthority> authorities = super.getGroupMembershipRoles(userDn, username);
        
        if (authorities.contains(adminRoleAsAuthority)) {
            authorities.add(adminAuthority);
            authorities.add(modelerAuthority);
            authorities.add(analystAuthority);
        }
        
        authorities.addAll(defaultAuthorities);
        
        return authorities;
    }
    
    
}
