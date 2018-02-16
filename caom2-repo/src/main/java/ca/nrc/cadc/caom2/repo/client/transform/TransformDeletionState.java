package ca.nrc.cadc.caom2.repo.client.transform;

import ca.nrc.cadc.caom2.DeletedObservation;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.persistence.Util;
import ca.nrc.cadc.date.DateUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;

public class TransformDeletionState extends Transformer {

    private static final Logger log = Logger.getLogger(TransformDeletionState.class);

    public TransformDeletionState(DateFormat dateFormat, char separator, char endOfLine) {
        super(dateFormat, separator, endOfLine);
    }

    public List<DeletedObservation> transformDeletedEntity(ByteArrayOutputStream bos) throws ParseException, IOException, URISyntaxException {
        // <Observation.id> <Observation.collection> <Observation.observationID> <timestamp>
        List<DeletedObservation> list = new ArrayList<>();

        String id = null;
        String observationID = null;
        String sdate;
        Date date = null;
        String collection = null;

        String aux = "";

        boolean readingDate = false;
        boolean readingCollection = false;
        boolean readingId = false;
        boolean readingObservationID = true;

        for (int i = 0; i < bos.toString().length(); i++) {
            char c = bos.toString().charAt(i);

            if (c != getSeparator() && c != getEndOfLine()) {
                aux += c;
            } else if (c == getSeparator()) {
                if (readingObservationID) {
                    readingObservationID = false;
                    readingCollection = true;
                    readingId = false;
                    readingDate = false;
                    observationID = aux;
                    aux = "";
                } else if (readingCollection) {
                    collection = aux;
                    // log.debug("*************** collection: " + collection);
                    readingObservationID = false;
                    readingCollection = false;
                    readingId = true;
                    readingDate = false;
                    aux = "";
                } else if (readingId) {
                    id = aux;
                    // log.debug("*************** id: " + id);
                    readingObservationID = false;
                    readingCollection = false;
                    readingId = false;
                    readingDate = true;
                    aux = "";
                } else if (readingDate) {
                    sdate = aux;
                    // log.debug("*************** sdate: " + sdate);
                    date = DateUtil.flexToDate(sdate, getDateFormat());
                    readingObservationID = false;
                    readingCollection = false;
                    readingId = false;
                    readingDate = false;
                    aux = "";
                }

            } else if (c == getEndOfLine()) {
                if (id == null || collection == null) {
                    continue;
                }

                sdate = aux;
                date = DateUtil.flexToDate(sdate, getDateFormat());

                // TODO: make call(s) to the deletion endpoint until requested number of entries (like getObservationList)

                // parse each line into the following 4 values, create DeletedObservation, and add to output list, eg:

                aux = "";

                if (id != null && observationID != null && collection != null && date != null) {
                    UUID uuid = UUID.fromString(observationID);
                    ObservationURI uri = new ObservationURI(collection, id);
                    DeletedObservation de = new DeletedObservation(uuid, uri);
                    Util.assignDeletedLastModified(de, date, "lastModified");
                    list.add(de);
                }
                readingCollection = false;
                readingId = false;
                readingObservationID = true;
                readingDate = false;
            }
        }

        return list;

    }

}
