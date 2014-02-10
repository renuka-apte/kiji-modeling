package org.kiji.modeling.framework

import com.twitter.scalding.{Args, Mode, TupleConversions, FieldConversions}

/**
 * Users should extend this while writing batch jobs for modeling.
 *
 * This is purely syntactic sugar. Users can just as easily write:
 *
 * class SampleJob
 *     extends ModelPipeConversions
 *     with FieldConversions
 *     with TupleConversions {...}
 */
class NewModelingJob(mode: Mode, args: Args)
    extends IterativePipeConversions
    with FieldConversions
    with TupleConversions
