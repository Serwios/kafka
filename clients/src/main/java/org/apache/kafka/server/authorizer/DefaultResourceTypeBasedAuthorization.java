package org.apache.kafka.server.authorizer;

import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;

import java.util.*;

public class DefaultResourceTypeBasedAuthorization {
    private DefaultResourceTypeBasedAuthorization() {}

    public static AuthorizationResult authorize(
            AuthorizableRequestContext requestContext,
            AclOperation op,
            ResourceType resourceType,
            Authorizer authorizer
    ) {
        SecurityUtils.authorizeByResourceTypeCheckArgs(op, resourceType);

        if (isSuperUserAuthorized(requestContext, op, resourceType, authorizer)) {
            return AuthorizationResult.ALLOWED;
        }

        Map<PatternType, Set<String>> denyPatterns = initializePatternMap();
        Map<PatternType, Set<String>> allowPatterns = initializePatternMap();

        boolean hasWildcardAllow = false;
        KafkaPrincipal principal = createKafkaPrincipal(requestContext);
        String hostAddr = requestContext.clientAddress().getHostAddress();

        for (AclBinding binding : authorizer.acls(createAclBindingFilter(resourceType))) {
            if (!isValidBinding(binding, principal, hostAddr, op)) {
                continue;
            }

            if (binding.entry().permissionType() == AclPermissionType.DENY) {
                processDenyAcl(binding, denyPatterns);
                continue;
            }

            if (binding.entry().permissionType() == AclPermissionType.ALLOW) {
                processAllowAcl(binding, allowPatterns);
                if (binding.pattern().name().equals(ResourcePattern.WILDCARD_RESOURCE)) {
                    hasWildcardAllow = true;
                }
            }
        }

        if (hasWildcardAllow) {
            return AuthorizationResult.ALLOWED;
        }

        return evaluateAuthorizationResult(allowPatterns, denyPatterns);
    }

    private static boolean isSuperUserAuthorized(AuthorizableRequestContext requestContext, AclOperation op, ResourceType resourceType, Authorizer authorizer) {
        return authorizer.authorize(requestContext, Collections.singletonList(new Action(
                        op, new ResourcePattern(resourceType, "hardcode", PatternType.LITERAL),
                        0, true, false)))
                .getFirst() == AuthorizationResult.ALLOWED;
    }

    private static Map<PatternType, Set<String>> initializePatternMap() {
        return new EnumMap<>(PatternType.class) {{
            put(PatternType.LITERAL, new HashSet<>());
            put(PatternType.PREFIXED, new HashSet<>());
        }};
    }

    private static KafkaPrincipal createKafkaPrincipal(AuthorizableRequestContext requestContext) {
        return new KafkaPrincipal(
                requestContext.principal().getPrincipalType(),
                requestContext.principal().getName());
    }

    private static AclBindingFilter createAclBindingFilter(ResourceType resourceType) {
        ResourcePatternFilter resourcePatternFilter = new ResourcePatternFilter(resourceType, null, PatternType.ANY);
        return new AclBindingFilter(resourcePatternFilter, AccessControlEntryFilter.ANY);
    }

    private static boolean isValidBinding(AclBinding binding, KafkaPrincipal principal, String hostAddr, AclOperation op) {
        return (binding.entry().host().equals(hostAddr) || binding.entry().host().equals("*"))
                && (SecurityUtils.parseKafkaPrincipal(binding.entry().principal()).equals(principal) || binding.entry().principal().equals("User:*"))
                && (binding.entry().operation() == op || binding.entry().operation() == AclOperation.ALL);
    }

    private static void processDenyAcl(AclBinding binding, Map<PatternType, Set<String>> denyPatterns) {
        switch (binding.pattern().patternType()) {
            case LITERAL:
                if (binding.pattern().name().equals(ResourcePattern.WILDCARD_RESOURCE)) {
                    throw new AuthorizationException("DENIED");
                }
                denyPatterns.get(PatternType.LITERAL).add(binding.pattern().name());
                break;
            case PREFIXED:
                denyPatterns.get(PatternType.PREFIXED).add(binding.pattern().name());
                break;
            default:
        }
    }

    private static void processAllowAcl(AclBinding binding, Map<PatternType, Set<String>> allowPatterns) {
        switch (binding.pattern().patternType()) {
            case LITERAL:
                allowPatterns.get(PatternType.LITERAL).add(binding.pattern().name());
                break;
            case PREFIXED:
                allowPatterns.get(PatternType.PREFIXED).add(binding.pattern().name());
                break;
            default:
        }
    }

    private static AuthorizationResult evaluateAuthorizationResult(Map<PatternType, Set<String>> allowPatterns, Map<PatternType, Set<String>> denyPatterns) {
        for (Map.Entry<PatternType, Set<String>> entry : allowPatterns.entrySet()) {
            for (String allowStr : entry.getValue()) {
                if (entry.getKey() == PatternType.LITERAL && denyPatterns.get(PatternType.LITERAL).contains(allowStr)) {
                    continue;
                }
                if (isDominatedByDenyPattern(allowStr, denyPatterns)) {
                    continue;
                }
                return AuthorizationResult.ALLOWED;
            }
        }
        return AuthorizationResult.DENIED;
    }

    private static boolean isDominatedByDenyPattern(String allowStr, Map<PatternType, Set<String>> denyPatterns) {
        StringBuilder sb = new StringBuilder();
        for (char ch : allowStr.toCharArray()) {
            sb.append(ch);
            if (denyPatterns.get(PatternType.PREFIXED).contains(sb.toString())) {
                return true;
            }
        }
        return false;
    }
}
