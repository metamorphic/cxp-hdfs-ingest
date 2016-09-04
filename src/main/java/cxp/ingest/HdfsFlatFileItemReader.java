package cxp.ingest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.util.Assert;
import org.springframework.validation.BindException;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by markmo on 7/04/15.
 */
public class HdfsFlatFileItemReader extends FlatFileItemReader<Map<String, Object>> {

    private static final Log log = LogFactory.getLog(HdfsFlatFileItemReader.class);

    private MetadataProvider metadataProvider;

    public void setPath(String path) {
        if (log.isDebugEnabled()) {
            log.debug("Setting path " + path);
        }
        String filename = path.substring(path.lastIndexOf('/') + 1);
        if (log.isDebugEnabled()) {
            log.debug("Filename " + filename);
        }
        metadataProvider.setFilename(filename);
        final FileDataset fileDataset = metadataProvider.getFileDataset();
        Pattern p = Pattern.compile("/test/");
        Matcher matcher = p.matcher(path);
        boolean isTestLoad = matcher.find();
        if (log.isDebugEnabled()) {
            log.debug("Is test load? " + (isTestLoad ? "YES" : "NO"));
        }
        metadataProvider.setTest(isTestLoad);
        metadataProvider.startJob();

        if (fileDataset == null) {
            throw new RuntimeException("No dataset found for '" + filename + "'");
        }

        if (fileDataset.isHeaderRow()) {
            setLinesToSkip(1);
        }

        final char quotechar;
        String textQualifier = fileDataset.getTextQualifier();
        if (textQualifier == null) {
            quotechar = ',';
        } else {
            quotechar = textQualifier.charAt(0);
        }
        Assert.notNull(fileDataset.getColumnNames());

        setLineMapper(new DefaultLineMapper<Map<String, Object>>() {{

            setLineTokenizer(new MetadataDrivenDelimitedLineTokenizer(fileDataset) {{
                setNames(fileDataset.getColumnNames());
                setQuoteCharacter(quotechar);
                setStrict(false);
            }});

            setFieldSetMapper(new FieldSetMapper<Map<String, Object>>() {
                @Override
                public Map<String, Object> mapFieldSet(FieldSet fieldSet) throws BindException {
                    if (fieldSet == null) return null;
                    Map<String, Object> fields = new HashMap<String, Object>();
                    if (fileDataset.getColumns() != null) {
                        for (FileColumn column : fileDataset.getColumns()) {
                            if ("integer".equals(column.getValueTypeName())) {
                                fields.put(column.getName(), fieldSet.readInt(column.getColumnIndex() - 1));
                            } else {
                                fields.put(column.getName(), fieldSet.readString(column.getColumnIndex() - 1));
                            }
                        }
                    }
                    return fields;
                }
            });
        }});
    }

    public void setMetadataProvider(MetadataProvider metadataProvider) {
        this.metadataProvider = metadataProvider;
    }
}