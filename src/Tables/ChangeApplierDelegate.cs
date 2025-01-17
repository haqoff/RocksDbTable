namespace RocksDbTable.Tables;

/// <summary>
/// The delegate used to perform the atomic update.
/// </summary>
/// <param name="primaryKey">The primary key.</param>
/// <param name="currentValue">The current value in the store. Or <c>null</c> if there was no value in the store.</param>
/// <param name="change">The change to apply.</param>
/// <param name="nextValue">
/// The new value to put into the store if the delegate returns <c>true</c>.
/// If the new value is <c>null</c>, delete will be performed.
/// The update allows the primary key to change.
/// </param>
/// <returns>
/// <c>True</c> if the change should be applied. <c>False</c> if the change should be ignored.
/// </returns>
/// <typeparam name="TPrimaryKey">Primary key type.</typeparam>
/// <typeparam name="TValue">Value type.</typeparam>
/// <typeparam name="TChange">Change type.</typeparam>
public delegate bool ChangeApplierDelegate<in TPrimaryKey, TValue, in TChange>(TPrimaryKey primaryKey, TValue? currentValue, TChange change, out TValue? nextValue);