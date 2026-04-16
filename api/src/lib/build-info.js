import { existsSync, readFileSync, statSync } from 'node:fs';
import path from 'node:path';

const rootDir = path.resolve(process.cwd());

const resolveGitDir = ({ startDir }) => {
  let currentDir = startDir;

  for (;;) {
    const gitPath = path.join(currentDir, '.git');
    if (existsSync(gitPath)) {
      const stats = statSync(gitPath);
      if (stats.isDirectory()) {
        return gitPath;
      }

      const content = readFileSync(gitPath, 'utf8').trim();
      if (content.startsWith('gitdir:')) {
        return path.resolve(currentDir, content.slice('gitdir:'.length).trim());
      }
    }

    const parentDir = path.dirname(currentDir);
    if (parentDir === currentDir) {
      return null;
    }

    currentDir = parentDir;
  }
};

const resolveHeadCommit = ({ gitDir }) => {
  if (!gitDir) {
    return null;
  }

  const headPath = path.join(gitDir, 'HEAD');
  if (!existsSync(headPath)) {
    return null;
  }

  const headContent = readFileSync(headPath, 'utf8').trim();
  if (!headContent) {
    return null;
  }

  if (!headContent.startsWith('ref:')) {
    return headContent.slice(0, 7);
  }

  const refName = headContent.slice('ref:'.length).trim();
  const refPath = path.join(gitDir, refName);
  if (existsSync(refPath)) {
    return readFileSync(refPath, 'utf8').trim().slice(0, 7);
  }

  const packedRefsPath = path.join(gitDir, 'packed-refs');
  if (!existsSync(packedRefsPath)) {
    return null;
  }

  for (const line of readFileSync(packedRefsPath, 'utf8').split('\n')) {
    if (!line || line.startsWith('#') || line.startsWith('^')) {
      continue;
    }

    const [commit, ref] = line.trim().split(' ');
    if (ref === refName && commit) {
      return commit.slice(0, 7);
    }
  }

  return null;
};

const resolveVersion = () => {
  const packagePath = path.join(rootDir, 'package.json');
  if (!existsSync(packagePath)) {
    return '0.0.0';
  }

  try {
    const parsed = JSON.parse(readFileSync(packagePath, 'utf8'));
    if (typeof parsed.version === 'string' && parsed.version.trim()) {
      return parsed.version.trim();
    }
  } catch {}

  return '0.0.0';
};

let cachedBuildInfo = null;

export const getBuildInfo = () => {
  if (cachedBuildInfo) {
    return cachedBuildInfo;
  }

  const version = resolveVersion();
  const commitHash = resolveHeadCommit({ gitDir: resolveGitDir({ startDir: rootDir }) }) ?? 'unknown';
  cachedBuildInfo = {
    version,
    commitHash,
    label: `v${version}-${commitHash}`
  };
  return cachedBuildInfo;
};
