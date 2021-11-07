# How to setup Teradata Instance

## Download and Install VMware `ovftool`

You need only `ovftool` from VMware, so it's good idea to look for this binary
in your distro repositories first.

For archlinux it's available in AUR: https://aur.archlinux.org/packages/vmware-ovftool/

If not, just download VMware installer:

```
curl -L https://www.vmware.com/go/getplayer-linux -O
chmod +x getplayer-linux
./getplayer-linux
```

## Download Teradata VMware Image

http://downloads.teradata.com/download/database/teradata-express-for-vmware-player

You need to register on their website to get image.

## Convert VMware Image into Virtualbox

Unzip downloaded archive and `cd` into unpacked directory.

```
ovftool *.vmx td.ovf
```

## Import Image into Virtualbox

First, use `--dry-run` to get understanding of default values that will
be used for this VM:

```
vboxmanage import td.ovf --vsys 0 --vmname teradata-database --dry-run
```

Then, do actual import without `--dry-run` (you can override default values
here).

Configure NAT, port-forwarding and VRDE (remote desktop):

```
vboxmanage modifyvm teradata-database --nic1 nat --natnet1 default
vboxmanage modifyvm teradata-database --natpf1 "teradata,tcp,,10250,,1025"
vboxmanage modifyvm teradata-database --natpf1 "ssh,tcp,,22000,,22"
vboxmanage modifyvm teradata-database --vrde on
```

## Start VM

```
vboxmanage startvm teradata-database --type headless
```

You can attach to VM screen using `vncviewer`:

```
vncviewer <HOST>:3389
```

At first run VM will fail to load X11 environment, you need to fix it by moving
(removing) X11 configuration for VMware (see next section how to access machine
via SSH).

You can just rename `xorg.conf` or remove it completely, it doesn't matter:

```
mv /etc/X11/xorg.conf /etc/X11/xorg.conf.vmware
```

Restart VM and now you will be able to use UI via `vncviewer`.

## SSH Access

Because we enabled port-forwarding at VM configuration, we can connect to VM
through host machine:

```
ssh  -p 22000 root@<HOST>
```

Default password for `root` is `root`.

First login will be likely very slow. To fix it, disable `UseDNS` in
`sshd_config` by adding `UseDNS no` option to file `/etc/ssh/sshd_config`.

Once we get SSH access, we can startup Teradata.

## Starting up Teradata

Most likely, Teradata will be in `DOWN` state.

To check it's state, run following command:

```
pdestate -a
```

Working instance should report following:

```
PDE state is RUN/STARTED.
DBS state is 4: Logons are enabled - Users are logged on
```

Stopped instance will report following:

```
PDE state: DOWN/HARDSTOP
```

To startup instance in `HARDSTOP` state, following steps are required:

```
rm -rf /var/opt/teradata/tdtemp/PanicLoopDetected
/etc/init.d/tpa start
```

## Create User and Database

`perm` is something like allowed database size for the user.

```
create database foodmart as perm = 4e+9;

create user test as perm = 4e+9, password = test;

grant all privileges on foodmart to test;

grant select on dbc.udtinfo to test;
```
