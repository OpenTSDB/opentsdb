package net.opentsdb.storage.json;

import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.tree.Branch;
import net.opentsdb.tree.Leaf;
import net.opentsdb.tree.Tree;
import net.opentsdb.tree.TreeRule;

import com.fasterxml.jackson.databind.module.SimpleModule;

public class StorageModule extends SimpleModule {
  public StorageModule() {
    super("StorageModule");
  }

  @Override
  public void setupModule(SetupContext context) {
    context.setMixInAnnotations(UIDMeta.class, UIDMetaMixIn.class);
    context.setMixInAnnotations(Annotation.class, AnnotationMixIn.class);
    context.setMixInAnnotations(TSMeta.class, TSMetaMixIn.class);
    context.setMixInAnnotations(TreeRule.class, TreeRuleMixIn.class);
    context.setMixInAnnotations(Tree.class, TreeMixIn.class);
    context.setMixInAnnotations(Branch.class, BranchMixIn.class);
    context.setMixInAnnotations(Leaf.class, LeafMixIn.class);
  }
}
