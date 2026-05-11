/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package ca.nrc.cadc.sia;

public class SiaUsageException extends Exception
{
    public SiaUsageException(String message)
    {
        super(message);
    }

    public SiaUsageException(String message, Throwable cause)
    {
        super(message, cause);
    }
    
}
