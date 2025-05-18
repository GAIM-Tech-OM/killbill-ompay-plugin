package org.killbill.billing.plugin.ompay;

import java.util.Optional;

import javax.inject.Named;
import javax.inject.Singleton;

import org.jooby.Result;
import org.jooby.mvc.GET;
import org.jooby.mvc.Local;
import org.jooby.mvc.Path;
import org.killbill.billing.plugin.core.resources.PluginHealthcheck;
import org.killbill.billing.tenant.api.Tenant;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.inject.Inject;

@Singleton
@Path("/healthcheck")
public class OmPayHealthcheckServlet extends PluginHealthcheck {
    private final OmPayHealthcheck healthcheck;

    @Inject
    public OmPayHealthcheckServlet(final OmPayHealthcheck healthcheck) {
        this.healthcheck = healthcheck;
    }

    @GET
    public Result check(@Local @Named("killbill_tenant") final Optional<Tenant> tenant) throws JsonProcessingException {
        return check(healthcheck, tenant.orElse(null), null);
    }
}