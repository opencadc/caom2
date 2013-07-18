// Created on 1-Mar-2006

package ca.nrc.cadc.caom2.types;

/**
 * TODO.
 * 
 * @version $Version$
 * @author pdowler
 */
public class IllegalPolygonException extends IllegalArgumentException
{
   private static final long serialVersionUID = 201112081300L;

    public IllegalPolygonException() { super(); }

    public IllegalPolygonException(String msg) { super(msg); }

    public IllegalPolygonException(String msg, Throwable cause) { super(msg, cause); }
}
