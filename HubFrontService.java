package ru.gkomega.router.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Service;
import ru.gkomega.common.model.RequestDataSource;
import ru.gkomega.router.data.ComposerDataHolder;
import ru.gkomega.router.exception.ComposerException;
import ru.gkomega.router.exception.RouterException;
import ru.gkomega.router.http_client.ComposerClient;
import ru.gkomega.router.http_client.ConnectorClient;
import ru.gkomega.router.model.composer.item.Connector;
import ru.gkomega.router.model.composer.item.DataSource;
import ru.gkomega.router.model.composer.item.DataSourcesObject;
import ru.gkomega.router.model.dto.hub_front_dto.GetDataDto;
import ru.gkomega.router.model.dto.internal.FullRequestInfo;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class HubFrontService {

    private static final String DATA_SOURCE_OBJECTS = "datasourcesobjects";
    private static final String DATA_SOURCES = "datasources";
    private static final String CONNECTOR = "connectors";
    private static final String COMMAND_GET_DATA = "GetData";

    private final ObjectMapper objectMapper;
    private final ComposerClient composerClient;
    private final ConnectorClient connectorClient;
    private final ComposerDataHolder composerDataHolder;

    public String getData(String project, GetDataDto getDataDto, Optional<Integer> skip, Optional<Integer> top) throws MalformedURLException, URISyntaxException, JsonProcessingException {
        val fullRequestInfo = requestDataCollector(project, getDataDto, skip, top);
        return connectorClient.doCall(fullRequestInfo.getConnectorName(), fullRequestInfo.getRequestDataSource());
    }

    public FullRequestInfo requestDataCollector(String project, GetDataDto getDataDto, Optional<Integer> skip, Optional<Integer> top)
            throws URISyntaxException {
        val uri = getComposerURI();
        val targetObjectUID = getDataDto.getDataObjectGuid();
        val object = getRecord(project, DATA_SOURCE_OBJECTS, targetObjectUID, DataSourcesObject.class, uri);
        val refDataSource = object.getRefDataSources();
        val source = getRecord(project, DATA_SOURCES, refDataSource, DataSource.class, uri);
        val refConnectorType = source.getRefConnector();
        val connector = getRecord(project, CONNECTOR, refConnectorType, Connector.class, uri);
        val connectorName = connector.getDescription();
        return FullRequestInfo
                .builder()
                .requestDataSource(buildRDSForData(getDataDto, object, source, skip, top))
                .connectorName(connectorName)
                .build();
    }

    private URI getComposerURI() throws URISyntaxException {
        return new URI(composerDataHolder.getSettings().getComposer());
    }

    private RequestDataSource buildRDSForData(GetDataDto getDataDto,
                                              DataSourcesObject object,
                                              DataSource source,
                                              Optional<Integer> skip,
                                              Optional<Integer> top) {

        List<String> columns = new ArrayList<>();
        if (Objects.nonNull(getDataDto.getColumns())) {
            columns = getDataDto.getColumns();
        }

        return RequestDataSource.builder().
                base(source.getApiBaseURL()).
                user(source.getAuthorizationLogin()).
                password(source.getAuthorizationPassword()).
                url(source.getHttpAddress()).
                schema(source.getApiSchemeURL()).
                table(object.getDescription()).
                columns(columns).
                commandType(COMMAND_GET_DATA).
                skip(skip.orElse(0)).
                top(top.orElse(Integer.MAX_VALUE)).
                build();

    }


    private <T> T getRecord(final String project, final String tableName, final String uuid, final Class<T> type, URI uri) {
        val composerAnswer = composerClient.table(
                project,
                tableName,
                uuid,
                uri
        );
        if (composerAnswer.getErr()) {
            throw new ComposerException(composerAnswer.getMsg());
        }
        val array = composerAnswer.getBody();
        if (array.isObject()) {
            throw new RouterException("Unexpected composer response.");
        }
        if (array.size() != 1) {
            throw new RouterException("Unexpected composer response. Array size: " + array.size());
        }
        return objectMapper.convertValue(array.get(0), type);
    }

}
