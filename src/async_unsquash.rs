use std::{
    collections::HashSet,
    os::unix::fs::PermissionsExt,
    path::{Component, Path},
};

use anyhow::{Context, Result};
use backhand::{FilesystemReader, InnerNode, Node, Squashfs, SquashfsFileReader, SquashfsSymlink};
use futures::{stream::FuturesUnordered, StreamExt};

pub async fn unsquash_tpcii_async(
    squashfs: impl AsRef<Path>,
    dest: impl AsRef<Path>,
    crates_filter: Option<HashSet<String>>,
) -> Result<()> {
    let (squashfs_path, dest) = (squashfs.as_ref().to_path_buf(), dest.as_ref().to_path_buf());

    anyhow::ensure!(
        matches!(tokio::fs::try_exists(&squashfs_path).await, Ok(true)),
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

    let filesystem = tokio::task::spawn_blocking(move || {
        let squashfs_f = std::fs::File::open(&squashfs_path)
            .with_context(|| format!("open squashfs '{}'", squashfs_path.display()))?;
        let squashfs_buf = std::io::BufReader::new(squashfs_f);
        let squashfs = Squashfs::from_reader(squashfs_buf)
            .with_context(|| format!("read squashfs '{}'", squashfs_path.display()))?;

        let filesystem = squashfs
            .into_filesystem_reader()
            .with_context(|| format!("convert squashfs to reader '{}'", squashfs_path.display()))?;
        Ok::<_, anyhow::Error>(filesystem)
    })
    .await
    .context("spawn blocking squashfs read task")??;

    let nodes: Vec<&Node<_>> = filesystem
        .files()
        .filter(|node| {
            crates_filter
                .as_ref()
                .map(|f| f.contains(&node.fullpath))
                .unwrap_or(true)
        })
        .collect();

    let mut futs: FuturesUnordered<_> = nodes
        .into_iter()
        .map(|node| extract_node(&dest, &filesystem, node))
        .collect();
    while let Some(res) = futs.next().await {
        res?;
    }

    Ok(())
}

#[inline]
async fn extract_node(
    root: impl AsRef<Path>,
    filesystem: &FilesystemReader<'_>,
    node: &Node<SquashfsFileReader>,
) -> anyhow::Result<()> {
    let path = &node.fullpath;
    let fullpath = path.strip_prefix(Component::RootDir).unwrap_or(path);
    let dest_path = root.as_ref().join(fullpath);

    tokio::fs::create_dir_all(
        dest_path
            .parent()
            .expect("path is guaranteed to contain a parent"),
    )
    .await
    .with_context(|| format!("create dir to unpack '{}'", dest_path.display()))?;

    match &node.inner {
        InnerNode::File(file) => {
            let fd = std::fs::File::create(&dest_path)
                .with_context(|| format!("create file to unpack: '{}'", dest_path.display()))?;
            let mut writer = std::io::BufWriter::with_capacity(file.basic.file_size as usize, &fd);
            let file = filesystem.file(&file.basic);
            let mut reader = file.reader();

            // FIXME: Move this into spawn_blocking. We cannot use `tokio::io::copy` because
            // SquashfsReadFile doesn't implement AsyncRead
            std::io::copy(&mut reader, &mut writer)
                .with_context(|| format!("extract file into '{}'", dest_path.display()))?;
            tokio::fs::set_permissions(&dest_path, std::fs::Permissions::from_mode(0o644))
                .await
                .with_context(|| format!("chmod 0o644 '{}'", dest_path.display()))?;
        }
        InnerNode::Symlink(SquashfsSymlink { link }) => unimplemented!(),
        InnerNode::Dir(_) => unimplemented!(),
        InnerNode::CharacterDevice(_) => unimplemented!(),
        InnerNode::BlockDevice(_) => unimplemented!(),
        InnerNode::NamedPipe => unimplemented!(),
        InnerNode::Socket => unimplemented!(),
    }

    Result::<(), anyhow::Error>::Ok(())
}
