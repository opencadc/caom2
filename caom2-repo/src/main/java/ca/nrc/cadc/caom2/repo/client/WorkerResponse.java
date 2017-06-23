package ca.nrc.cadc.caom2.repo.client;

import ca.nrc.cadc.caom2.Observation;

public class WorkerResponse {

    private Observation observation = null;
    private ObservationState observationState = null;
    private Exception error = null;

    public WorkerResponse(Observation obs, ObservationState obsState, Exception err) {
        this.setObservation(obs);
        this.setObservationState(obsState);
        this.setError(err);
    }

    public Observation getObservation() {
        return observation;
    }

    public void setObservation(Observation observation) {
        this.observation = observation;
    }

    public ObservationState getObservationState() {
        return observationState;
    }

    public void setObservationState(ObservationState observationState) {
        this.observationState = observationState;
    }

    public Exception getError() {
        return error;
    }

    public void setError(Exception error) {
        this.error = error;
    }

    @Override
    public String toString() {
        return observation == null ? "null"
                : observation.getObservationID() + " " + observationState == null ? "null"
                        : observationState.getCollection() + " " + error == null ? "Correct"
                                : error.getMessage();
    }

}
