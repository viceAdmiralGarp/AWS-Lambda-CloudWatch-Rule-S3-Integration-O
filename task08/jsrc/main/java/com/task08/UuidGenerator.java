package com.task08;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.events.RuleEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@LambdaHandler(
        lambdaName = "uuid_generator",
        roleName = "uuid_generator-role",
        isPublishVersion = true,
        aliasName = "${lambdas_alias_name}",
        logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@RuleEventSource(
        targetRule = "uuid_trigger"
)
@EnvironmentVariables(value = {
        @EnvironmentVariable(key = "target_bucket", value = "${target_bucket}"),
        @EnvironmentVariable(key = "region", value = "${region}")
})
public class UuidGenerator implements RequestHandler<ScheduledEvent, Void> {

    private static final AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
            .withRegion(System.getenv("region"))
            .build();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Void handleRequest(ScheduledEvent event, Context context) {
        try {

            List<String> uuids = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                uuids.add(UUID.randomUUID().toString());
            }

            String fileName = Instant.now().toString();
            String content = objectMapper.writeValueAsString(new UuidResponse(uuids));

            s3Client.putObject(
                    System.getenv("target_bucket"),
                    fileName,
                    content
            );

        } catch (Exception e) {
            context.getLogger().log("Error: " + e.getMessage());
        }
        return null;
    }

    private static class UuidResponse {
        private List<String> ids;

        public UuidResponse(List<String> ids) {
            this.ids = ids;
        }

        public List<String> getIds() {
            return ids;
        }
    }
}