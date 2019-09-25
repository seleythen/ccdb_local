package ch.alice.o2.ccdb.servlets;

/**
 * @author costing
 * @since 2019-09-25
 */
public interface SQLNotifier {
	/**
	 * Callback after a new object was created
	 * 
	 * @param object
	 */
	public void newObject(SQLObject object);

	/**
	 * Callback after this object was modified
	 * 
	 * @param object
	 */
	public void updatedObject(SQLObject object);

	/**
	 * Callback after this object was removed
	 * 
	 * @param object
	 */
	public void deletedObject(SQLObject object);
}
