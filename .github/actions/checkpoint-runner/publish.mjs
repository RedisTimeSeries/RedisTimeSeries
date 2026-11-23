import { DefaultArtifactClient } from '@actions/artifact';
import path from 'node:path';

const [name, file] = process.argv.slice(2);
await new DefaultArtifactClient().uploadArtifact(name, [file], path.dirname(file), {
  retentionDays: 7,
});
