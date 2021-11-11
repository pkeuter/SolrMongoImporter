package org.apache.solr.handler.dataimport;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.jayway.jsonpath.*;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author justin.spies
 *
 */
@SuppressWarnings({ "PMD.ModifiedCyclomaticComplexity", "PMD.StdCyclomaticComplexity",
    "PMD.CyclomaticComplexity" })
public class MongoMapperTransformer extends Transformer {
  private static final Logger LOG = LoggerFactory.getLogger(MongoMapperTransformer.class);

  @Override
  @SuppressWarnings({ "PMD.AvoidInstantiatingObjectsInLoops", "PMD.CommentRequired" })
  public Object transformRow(final Map<String, Object> row, final Context context) {
    LOG.debug("Transforming row: " + row);
    Document docRow = new Document(row);
    DocumentContext document = JsonPath.parse(docRow.toJson());

    for (final Map<String, String> field : context.getAllEntityFields()) {
      Object buf;
      final String jsonPath = field.get(JSONPATH);
      if (jsonPath != null && document != null) {
        try {
          buf = document.read(jsonPath);
        } catch (PathNotFoundException e) {
          LOG.warn("Error reading path " + jsonPath + ": " + e.getMessage());
          buf = row.get(field.get(DataImporter.COLUMN));
        }

        if (buf instanceof ObjectId) {
          buf = ((ObjectId) buf).toHexString();
        } else if (buf instanceof List && ((List<?>) buf).getClass().equals(ObjectId.class)) {
          final List<String> list = new ArrayList<String>();
          for (final Object e : (List<?>) buf) {
            list.add(((ObjectId) e).toHexString());
          }
          buf = list;
        }
      } else {
        buf = row.get(field.get(DataImporter.COLUMN));
        if (buf instanceof ObjectId) {
          buf = ((ObjectId) buf).toHexString();
        }
      }
      row.put(field.get(DataImporter.COLUMN), buf);
    }

    LOG.debug("Row to be returned is " + row);
    return row;
  }

  public static final String JSONPATH = "jsonpath";
}