"""
Instead of interacting with Dolt directly, we access an interface that Dolt implements.

This is because there are multiple possible ways to use Dolt to implement the necessary features.

Dolt concepts like branches and commit history get abstracted away.

For example, remote-dataset pairs need to map onto branches.

There are multiple copies of each dataset, one for each remote.

One of the current obstacles is that we need branches in order to merge.

"""
