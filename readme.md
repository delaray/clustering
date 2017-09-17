# understanding

this system spins up ec2 instances to run batch jobs, terminates them when done. it reads from s3 and writes to s3.

everything you need to operate this system is in `bin/`.

everything you need to understand what this system does is in `acceptance/`.

# using

##### amis

everything is expecting to be run on an ami built via `bash bin/build_ami.sh`, including that script itself. the latest version of that ami can be found with `ec2 amis audience-explorer|head -n1`, and is currently `ami-e9a0b783`. if can also be found by looking in the aws console for amis named `audience-explorer-$DATE`.

##### keys

the machine running the pipeline, which could be ec2 or your laptop, is going to require the ssh private key for the ec2 keypair name specified in the `bin/build_ami.sh`. currently that keypair is `20131104_platform`. copy that private key file to `~/.ssh/id_rsa` and then `chmod 600 ~/.ssh/id_rsa`.

##### invocation

to run the whole pipeline (usually what you want), from raw audience data to clustering output, use:
 - `python3.4 bin/pipeline.py -h`

to run an individual component of the pipeline (not usually what you want), use:
 - `python3.4 bin/group_by_user.py -h`
 - `python3.4 bin/make_clustering_inputs.py -h`
 - `python3.4 bin/kmeans.py -h`

currently, there is no need to run spark on an actual cluster, running it on large machines in `local[*]` is sufficient. if you want to run on a cluster, use:
 - `python3.4 bin/kmeans_cluster.py -h`

to generate new category files, copy `bin/cats.py` to `/tmp` on a box with a working `shareablee/web` install, run it, and stick the results in s3 somewhere. `production-cron` is a box that is good for this kind of thing.

# developing

run all tests: `ENABLE_SCHEMA=y TEST_SPARK=y lein with-profile kmeans test`

run non-spark tests: `ENABLE_SCHEMA=y lein test`

run acceptance tests: `python3.4 acceptance/pipeline_single_month.py`

##### important changes

[changes.md](https://github.com/shareablee/audience-explorer/blob/master/changes.md)

# misc

##### pipeline notes

for a new month, you want to run the whole `pipeline`, generating new `group-by-user`, `make-clustering-inputs`, and `kmeans` data for each new year-month and category.

for tweaking params on clustering, you dont need to recompute anything but the clustering result, ie only 1/3 of the `pipeline` needs to run. `pipeline` caches its outputs, so if an output dir exists, that computation is a noop. this means if you run the `pipeline` twice with the same params, the second run will do nothing. you have two options for doing additional clustering runs to tweak parameters:
 - use `bin/kmeans.py` and explicitly specify the input, with the output going to some tmp location
 - use `bin/pipeline.py` again, modifying the `--params` and adding a `--suffix`. this suffix is appended to the outputdir for the `kmeans` step, which means that `kmeans` is no longer a noop because its output dir doesnt exist! now new outputs live in the same root, with a suffix to differentiate them from old outputs.

##### py-aws notes

[py-aws](https://github.com/nathants/py-aws) is currently used as a library in the python entrypoints, including the pipeline. when running any of them, you will see `pipeline=<id>` and `launch=<id>` in the logs. these are tags, which are use for the ec2 instances and their logs. py-aws exposes two entrypoints, `ec2 -h` and `launch -h`. you can do things like `ec2 ls launch=<id>` and `launch wait launch=<id>`. other launch commands which can be useful for debugging are `params`, `restart`, `log`, and `logs`. explore the signatures with `launch logs -h`.

# dependencies

dependencies have all already been built, and are in our private s3 repo as specified in project.clj. if you want to rebuild and publish to our private s3, do the following.

##### spark

spark must be custom built to enable openblas, with --Pnetlib-lgpl

run: `bash bin/build_spark.sh`

##### other snapshots

run: `bash bin/build_libs.sh`
"# clustering" 
