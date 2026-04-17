"""
Instead of interacting with Dolt directly, we access an interface that Dolt implements.

This is because there are multiple possible ways to use Dolt to implement the necessary features.

Dolt concepts like branches and commit history get abstracted away.

For example, remote-dataset pairs need to map onto branches.

There are multiple copies of each dataset, one for each remote.

One of the current obstacles is that we need branches in order to merge.

An abstract "versioned database" provides a way to diff and merge different versions of a table without directly using Dolt.
It requires the following methods:

- 
- diff(dataset, table, repo1, repo2)



"""
