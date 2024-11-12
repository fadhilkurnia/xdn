package edu.umass.cs.xdn.service;

import edu.umass.cs.xdn.interfaces.behavior.RequestBehaviorType;
import io.netty.handler.codec.http.HttpMethod;

import java.util.List;
import java.util.Set;

public class RequestMatcher {
    private final String matcherName;  // can be null
    private final String pathPrefix;
    private final List<String> httpMethods;
    private final RequestBehaviorType behavior;

    public RequestMatcher(String matcherName, String pathPrefix, List<String> httpMethods,
                          RequestBehaviorType behavior) {
        // Validates the given prefix
        assert pathPrefix != null && pathPrefix.startsWith("/") :
                "pathPrefix must start with '/";

        // Validates the given methods
        assert !httpMethods.isEmpty() : "httpMethods cannot be empty";
        Set<String> validMethods = Set.of(HttpMethod.GET.name(), HttpMethod.POST.name(),
                HttpMethod.PUT.name(), HttpMethod.DELETE.name(), HttpMethod.HEAD.name(),
                HttpMethod.OPTIONS.name(), HttpMethod.CONNECT.name(), HttpMethod.PATCH.name(),
                HttpMethod.TRACE.name());
        for (String method : httpMethods) {
            assert validMethods.contains(method) :
                    "provided method=" + method + " is not a valid HTTP method";
        }

        // Validates the behavior
        assert behavior != null;

        this.matcherName = matcherName;
        this.pathPrefix = pathPrefix;
        this.httpMethods = httpMethods;
        this.behavior = behavior;
    }
}
