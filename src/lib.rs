use std::{
    collections::HashSet,
    os::unix::fs::PermissionsExt,
    path::{Component, Path},
};

use anyhow::{Context, Result};
use backhand::{FilesystemReader, InnerNode, Node, Squashfs, SquashfsFileReader, SquashfsSymlink};

pub fn unsquash_tpcii_blocking(
    squashfs: impl AsRef<Path>,
    dest: impl AsRef<Path>,
    crates_filter: Option<HashSet<String>>,
) -> Result<()> {
    use rayon::prelude::*;

    let (squashfs_path, dest) = (squashfs.as_ref(), dest.as_ref());

    anyhow::ensure!(
        squashfs_path.exists(),
        "specified squashfs archive does not exist: '{}'",
        squashfs_path.display(),
    );

    let crates_filter = crates_filter.map(|filter| {
        filter
            .into_iter()
            .filter_map(|krate| {
                let index_path = Path::new("/index").join(&krate);
                let salt_path = Path::new("/salts").join(&krate);

                let paths_iter = index_path
                    .ancestors()
                    .chain(salt_path.ancestors())
                    .map(|p| p.to_path_buf())
                    .collect::<Vec<_>>();
                Some(paths_iter)
            })
            .flatten()
            .collect::<HashSet<_>>()
    });

    if crates_filter.as_ref().is_some_and(|f| f.is_empty()) {
        return Ok(());
    }

    let squashfs_f = std::fs::File::open(squashfs_path)
        .with_context(|| format!("open squashfs '{}'", squashfs_path.display()))?;
    let squashfs_buf = std::io::BufReader::new(squashfs_f);
    let squashfs = Squashfs::from_reader(squashfs_buf)
        .with_context(|| format!("read squashfs '{}'", squashfs_path.display()))?;

    let filesystem = squashfs
        .into_filesystem_reader()
        .with_context(|| format!("convert squashfs to reader '{}'", squashfs_path.display()))?;

    let nodes: Vec<&Node<_>> = filesystem
        .files()
        .filter(|node| {
            crates_filter
                .as_ref()
                .map(|f| f.contains(&node.fullpath))
                .unwrap_or(true)
        })
        .collect();

    nodes
        .into_par_iter()
        .try_for_each(|node| extract_node_blocking(dest, &filesystem, node))
}

#[inline]
fn extract_node_blocking(
    root: impl AsRef<Path>,
    filesystem: &FilesystemReader<'_>,
    node: &Node<SquashfsFileReader>,
) -> anyhow::Result<()> {
    let path = &node.fullpath;
    let fullpath = path.strip_prefix(Component::RootDir).unwrap_or(path);
    let dest_path = root.as_ref().join(fullpath);

    std::fs::create_dir_all(
        dest_path
            .parent()
            .expect("path is guaranteed to contain a parent"),
    )
    .with_context(|| format!("create dir to unpack '{}'", dest_path.display()))?;

    match &node.inner {
        InnerNode::File(file) => {
            let fd = std::fs::File::create(&dest_path)
                .with_context(|| format!("create file to unpack: '{}'", dest_path.display()))?;
            let mut writer = std::io::BufWriter::with_capacity(file.basic.file_size as usize, &fd);
            let file = filesystem.file(&file.basic);
            let mut reader = file.reader();

            std::io::copy(&mut reader, &mut writer)
                .with_context(|| format!("extract file into '{}'", dest_path.display()))?;
            std::fs::set_permissions(&dest_path, std::fs::Permissions::from_mode(0o644))
                .with_context(|| format!("chmod 0o644 '{}'", dest_path.display()))?;
        }
        InnerNode::Symlink(SquashfsSymlink { link }) => {
            std::os::unix::fs::symlink(link, &dest_path)
                .with_context(|| format!("symlink file into '{}'", dest_path.display()))?;
            lchmod(&dest_path, &std::fs::Permissions::from_mode(0o644))
                .with_context(|| format!("lchmod 0o644 '{}'", dest_path.display()))?;
        }
        InnerNode::Dir(_) => {
            std::fs::create_dir_all(&dest_path)
                .with_context(|| format!("create dir into '{}'", dest_path.display()))?;
            std::fs::set_permissions(&dest_path, std::fs::Permissions::from_mode(0o755))
                .with_context(|| format!("chmod 0o755 '{}'", dest_path.display()))?;
        }
        InnerNode::CharacterDevice(_) => unimplemented!(),
        InnerNode::BlockDevice(_) => unimplemented!(),
        InnerNode::NamedPipe => unimplemented!(),
        InnerNode::Socket => unimplemented!(),
    }

    Result::<(), anyhow::Error>::Ok(())
}

fn lchmod(symlink: impl AsRef<std::path::Path>, mode: &std::fs::Permissions) -> anyhow::Result<()> {
    use nix::{fcntl, sys::stat};
    use std::os::unix::fs::PermissionsExt;

    let path = symlink.as_ref();
    let mode = stat::Mode::from_bits_truncate(mode.mode());

    anyhow::ensure!(
        path.is_symlink(),
        "path '{}' is not a symlink, cannot lchmod",
        path.display()
    );

    let dir = path
        .parent()
        .with_context(|| format!("get parent of symlink '{}'", path.display()))?;
    let filename = path
        .file_name()
        .with_context(|| format!("get filename of symlink '{}'", path.display()))?;

    let dir_fd = fcntl::open(dir, fcntl::OFlag::empty(), stat::Mode::empty())
        .with_context(|| format!("open dir '{}'", path.display()))?;

    stat::fchmodat(
        Some(dir_fd),
        filename,
        mode,
        stat::FchmodatFlags::NoFollowSymlink,
    )
    .with_context(|| format!("fchmodat {:#o} of symlink '{}'", mode, path.display()))
}
