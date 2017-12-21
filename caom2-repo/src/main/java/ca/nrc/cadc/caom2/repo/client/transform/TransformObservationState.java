package ca.nrc.cadc.caom2.repo.client.transform;

import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.date.DateUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class TransformObservationState extends Transformer {

    public TransformObservationState(DateFormat dateFormat, char separator, char endOfLine) {
        super(dateFormat, separator, endOfLine);
    }

    public List<ObservationState> transformObservationState(ByteArrayOutputStream bos) throws ParseException, IOException, URISyntaxException {
        // <Observation.collection> <Observation.observationID> <Observation.maxLastModified> <Observation.accMetaChecksum>  (version 2.3+)
        List<ObservationState> list = new ArrayList<>();

        String id = null;
        String sdate;
        Date date = null;
        String collection = null;
        String md5;
        String aux = "";

        boolean readingDate = false;
        boolean readingCollection = true;
        boolean readingId = false;

        for (int i = 0; i < bos.toString().length(); i++) {
            char c = bos.toString().charAt(i);

            if (c != ' ' && c != getSeparator() && c != getEndOfLine()) {
                aux += c;
            } else if (c == getSeparator()) {
                if (readingCollection) {
                    collection = aux;
                    // log.debug("*************** collection: " + collection);
                    readingCollection = false;
                    readingId = true;
                    readingDate = false;
                    aux = "";
                } else if (readingId) {
                    id = aux;
                    // log.debug("*************** id: " + id);
                    readingCollection = false;
                    readingId = false;
                    readingDate = true;
                    aux = "";
                } else if (readingDate) {
                    sdate = aux;
                    // log.debug("*************** sdate: " + sdate);
                    date = DateUtil.flexToDate(sdate, getDateFormat());

                    readingCollection = false;
                    readingId = false;
                    readingDate = false;
                    aux = "";
                }

            } else if (c == ' ') {
                if (readingDate) {
                    sdate = aux;
                    // log.debug("*************** sdate: " + sdate);
                    date = DateUtil.flexToDate(sdate, getDateFormat());

                    readingCollection = false;
                    readingId = false;
                    readingDate = false;
                    aux = "";
                }
            } else if (c == getEndOfLine()) {
                if (id == null || collection == null) {
                    continue;
                }

                ObservationState os = new ObservationState(new ObservationURI(collection, id));

                if (date == null) {
                    sdate = aux;
                    date = DateUtil.flexToDate(sdate, getDateFormat());
                }

                os.maxLastModified = date;

                md5 = aux;
                aux = "";
                // log.debug("*************** md5: " + md5);
                if (!md5.equals("")) {
                    os.accMetaChecksum = new URI(md5);
                }

                list.add(os);
                readingCollection = true;
                readingId = false;
            }
        }
        Collections.sort(list, getComparator());
        return list;
    }

}
