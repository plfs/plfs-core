The files in this contrib directory are related to changing the buffer size
that FUSE uses. Being able to change the buffer size will improve PLFS
performance through FUSE. By default, FUSE breaks up IO requests in to chunks
of size 128 KB. This means that even if an application requests chunk sizes of
larger than 128 KB, FUSE will break those up and PLFS will only see requests
of size 128 KB. This smaller request size will cause performance problems when
using PLFS through the FUSE interface.

The following procedure will allow a user to change the buffer sizes used by
FUSE. Knowledge of generating rpms would be helpful to have before attempting
this process.

Here are the steps to creating the kernel and fuse rpms:

1)  Download the src rpm for the distribution. Only TOSS kernels have been
    tested by the PLFS team at the time of this writing.
2)  Make sure your rpm building area is set up (~/.rpmmacros). Make sure the
    following are set:
    %__builddir: where rpm will build the source
    %__rpmdir: where rpm will put the completed rpms
    %__srcrpmdir: where rpm will put source rpms
    %__sourcedir: where rpm can find package tarballs and patch files
    %__specdir: where rpm can find spec files
3)  Install the src rpm:
    $> rpm -ivh /path/to/kernel.src.rpm
    This will extract the spec file and place it in <__specdir>, extract source
    tarballs and patches and put them in <__sourcedir>.
4)  Copy over the appropriate kernel patch:
    $> cp kernel-fuse-chunk-size.patch.[4|8|12]MB <__sourcedir>/kernel-fuse-chunk-size.patch
5)  Double check that the patch can be applied. The included patches were
    developed against the kernel shipped with TOSS 2.1-3 which was provided by
    the kernel-2.6.32-358.11.1.2chaos.ch5.1.src.rpm. They may apply against
    other kernels.
    a) Go to the source directory and untar the kernel source:
       $> cd <__sourcedir>
       $> tar xjf <kernel source tarball>
    b) Do a dry run of the patch process to see how it will apply:
       $> cd <kernel source directory>
       $> patch -p1 --dry-run < <__sourcedir>/kernel-fuse-chunk-size.patch
    c) If the patch applies cleanly or there are warnings about offsets, the
       patch is ok to use as-is. If there are errors, it will be necessary to
       regenerate the patch for that particular kernel. Do this by copying the
       kernel source directory to a new directory:
       $> cd <__sourcedir>
       $> cp -r <kernel source directory> <kernel source directory>.orig
       Manually mak the changes in <kernel source directory> using the
       existing patch file as a template. Generate the patch using diff on
       the original and modified source directory trees:
       $> diff -Naur <kernel source directory>.orig \
          <kernel source directory> > \
          <__sourcedir>/kernel-chunk-size.patch
6)  Edit the kernel spec file to include the patch file. The spec file should
    be in %{__specdir}. For TOSS kernels, it is named kernel.spec. Add a line
    like the following in the patch section:
    Patch#: kernel-fuse-chunk-size.patch
    Replace the '#' with an appropriate number (one more than the last patch's
    number).
7)  Edit the kernel spec to add a string to the generated rpms' name. In the
    TOSS kernel, this is done near the top by uncommenting the line that
    defines 'buildid'. By doing this, the name of the kernel rpms will imply
    that they are not official. For example, the normal name of a TOSS kernel
    rpm might end something like this:
    chaos.ch5.1.x86_64.rpm
    With buildid uncommented, it will instead end like this:
    chaos.ch5.1.local.x86_64.rpm
8)  Build the kernel rpms:
    $> rpmbuild -ba <__specdir>/kernel.spec
    This may take quite some time. Note that the build should be done on a
    machine with physical hardware. This is due to the signing of the kernel
    files; the build process will use 'randomness' in physical hardware to come
    up with an acceptable key.
    If there is an error, it will be reported to the screen. Fix the errors and
    then try again. It might be necessary to remove a previous build attempt.
    Look in %__builddir and remove the kernel directory.
9)  Download the src rpm for the fuse library.
10) Install the fuse src rpm:
    $> rpm -ivh /path/to/fuse.src.rpm
11) Copy over the appropriate fuse patch:
    $> cp fuse-chunk_size.patch.[4|8|12]MB <__sourcedir>/fuse-chunk_size.patch
12) Repeat the directions in step 5 to verify that the patch can be applied
    cleanly or regenerate the patch for the given kernel, replacing kernel-
    specific names and info with the appropriate fuse-specific names and info.
    The FUSE patch was generated against TOSS 2.1-3's FUSE which was provided
    by the fuse-2.8.3-4.el6.src.rpm.
13) Edit <__specdir>/fuse.spec and add a patch line in the patch section which
    looks like the following:
    Patch#: fuse-chunk_size.patch
14) Edit <__specdir>/fuse.spec to use the same string used in step 7 to
    differentiate these fuse rpms from official fuse rpms. In the TOSS fuse
    spec file, this can be done by modifying the 'Release:' line near the top
    of the spec file. Instead of just having the iteration of the rpm itself,
    add the string. If the original line was this:
    Release: 5{?dist}
    change it to this:
    Release: 5<string from step 6>{?dist}
    This way it is easy to tell which generated fuse rpm should go with a
    particular set of generated kernel rpms.
15) Build the fuse rpms:
    $> rpmbuild -ba <__specdir>/fuse.spec
    This will not take long at all to build.

Now all of the needed rpms have been generated. It is now necessary to boot in
to the newly generated kernel. At LANL, this requires working with the admins
to generate a new image using the kernel rpms. The new fuse rpms also need to
be installed, although they are not as difficult to install as the kernel rpms.

Once booted in to the new kernel, it is possible to build other packages that
depend on the target kernel running. In other words, now is the time to build
other packages that need to be built against the modified kernel. Here is a
list of packages that we know need to built against the modified kernel:

* Lustre

Note that PLFS does not need to built against the new kernel.

Install the new rpms in the same manner as the fuse rpms.

It should be pointed out that running a host with a modified kernel in this
way may lead to undesirable behavior. Not all packages on the host will have
been recompiled against the new kernel, so it is possible that segmentation
faults may result if a package is used. It is recommended that the minimum
number of packages be rebuilt and no other work other than PLFS work be done.
