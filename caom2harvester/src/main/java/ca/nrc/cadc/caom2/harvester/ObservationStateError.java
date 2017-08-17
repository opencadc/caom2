package ca.nrc.cadc.caom2.harvester;

import ca.nrc.cadc.caom2.ObservationState;

public class ObservationStateError implements Comparable<ObservationStateError>
{
    private ObservationState obs = null;
    private String error = null;

    public ObservationStateError(ObservationState o, String e)
    {
        obs = o;
        error = e;
    }

    public ObservationState getObs()
    {
        return obs;
    }

    public String getError()
    {
        return error;
    }

    @Override
    public String toString()
    {
        String res = "";
        if (obs != null)
        {
            res = obs.getURI().getURI() + ": " + error;
        }
        return res;
    }

    @Override
    public int compareTo(ObservationStateError o)
    {
        return this.obs.getURI().compareTo(o.getObs().getURI());
    }

}
