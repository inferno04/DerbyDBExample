import java.io.InputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;
import org.apache.derby.jdbc.EmbeddedDriver;

/**
	DbShell.java
		Erik Ginter
		Date of last revision:	9-29-2016

	Compilation:
		derby.jar must be on the classpath.

	Usage:
		Enter SQL at the command line. Semicolon ';' terminates a statement.
		Enter an empty query ( no characters ) followed by a semicolon ';' to exit the shell.
**/
public class DbShell
{
	public static final String JDBC_URL = JDBCUrl ( "res/rootdb" );		//	Embedded database will be created within /res/ with name 'rootdb'

	public static String JDBCUrl ( final String dbName )
	{
		return ( "jdbc:derby:" + dbName + ";create=true" );		//	Remove 'create=true' if failure on db missing is desired
	}

	/**
		Print a result set as an HTML table for pretty-printing
			Leverage existing tech; don't re-invent this wheel.

		TODO:	Make error stream a parameter
	**/
	public static void printHTMLTable ( final PrintStream output, final String queryString, final ResultSet results )
	{
		try
		{
			final ResultSetMetaData resultsMeta = results.getMetaData ();
			try
			{
				final int columns = resultsMeta.getColumnCount ();

				output.println ( "<table>" );
				{
					// Print the table caption ( use the query to generate the table )
					{
						output.print ( '\t' );
						output.print ( "<caption>" );
						output.print ( queryString );
						output.println ( "</caption>" );
					}

					//	Print the column headers
					{
						output.print ( '\t' );
						output.print ( "<tr>" );
						for ( int c = 1; c <= columns; ++c )
						{
							try
							{
								output.print ( "<th>" );
								{
									final String columnName = resultsMeta.getColumnName ( c );
									output.print ( columnName );

									final String columnLabel = resultsMeta.getColumnLabel ( c );
									if ( ! columnLabel.equals ( columnName ) )
									{
										output.print ( " ( " );
										output.print ( columnLabel );
										output.print ( " )" );
									}
								}
								output.print ( "</th>" );
							}
							catch ( final SQLException ex ) { System.err.println ( ex ); }
						}
						output.println ( "</tr>" );
					}

					//	Print the table contents record-by-record
					try
					{
						while ( results.next () )
						{
							output.print ( '\t' );
							output.print ( "<tr>" );
							for ( int c = 1; c <= columns; ++c )
							{
								try
								{
									final String val = results.getString ( c );
									output.print ( "<td>" + val + "</td>" );
								}
								catch ( final SQLException ex ) { System.err.println ( ex ); }
							}
							output.println ( "</tr>" );
						}
					}
					catch ( final SQLException ex ) { System.err.println ( ex ); }
				}
				output.println ( "</table>" );
			}
			catch ( final SQLException ex ) { System.err.println ( ex ); }
		}
		catch ( final SQLException ex ) { System.err.println ( ex ); }
	}

	/**
		Create an interactive SQL shell.
		Semicolons terminate each statement.
		To exit, type zero-or-more whitespace characters followed by a single semicolon. ( empty statement )

		Errors + Exceptions printed to stderr for quick debugging.
		TODO:	Make error stream a parameter
	**/
	public static void shell ( final InputStream input, final PrintStream output )
	{
		final Scanner scanner = new Scanner ( input );
		final Driver driver = new EmbeddedDriver ();
		try
		{
			if ( driver.acceptsURL ( JDBC_URL ) )
			{
				try ( final Connection connection = driver.connect ( JDBC_URL, null ) )
				{
					final StringBuilder builder = new StringBuilder ();
					boolean emptyInput = true;
					while ( true )
					{
						final String line = ( scanner.hasNextLine () ) ? scanner.nextLine () : ";";
						if ( ";".equals ( line ) )
						{
							if ( emptyInput ) break;	// Semicolon by itself - exit the shell

							final String query = builder.toString ();
							builder.setLength ( 0 );
							emptyInput = true;

							try ( final Statement stmt = connection.createStatement () )
							{
								try
								{
									if ( query.startsWith ( "select " ) || query.startsWith ( "select\n" ) )
									{
										final ResultSet results = stmt.executeQuery ( query );
										printHTMLTable ( output, query, results );
									}
									else
									{
										stmt.execute ( query );
									}
								}
								catch ( final SQLException ex ) { System.err.println ( ex ); }
							}
							catch ( final SQLException ex ) { System.err.println ( ex ); }
						}
						else if ( "".equals ( line ) ) {}	// Eat empty lines
						else
						{
							if ( ! emptyInput )
							{
								builder.append ( ' ' );
							}

							emptyInput = false;
							builder.append ( line );
						}
					}
				}
				catch ( final SQLException ex ) { System.err.println ( ex ); }
			}
		}
		catch ( final SQLException ex ) { System.err.println ( ex ); }
	}

	public static void main ( final String[] args )
	{
		final InputStream input = System.in;
		final PrintStream output = System.out;

		shell ( input, output );
	}
}
