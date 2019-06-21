package dio.datamodel;

/**
 * Created by IgorPc on 6/4/2019.
 */
public class Reference {



    private String source;
    private String context;
    private String contextType;
    private String target;
    private String constructor;

    public Reference(String source, String context, String contextType, String target, String constructor) {
        this.source = source;
        this.context = context;
        this.contextType = contextType;
        this.target = target;
        this.constructor = constructor;
    }

    public Reference() {
    }


    public static class TopicWords {
        private String source;
        private String context;
        private String target;


        public TopicWords(String source, String context,String target) {
            this.source = source;
            this.context = context;
            this.target=target;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TopicWords that = (TopicWords) o;

            if (!source.equals(that.source)) return false;
            return context.equals(that.context);
        }

        @Override
        public int hashCode() {
            int result = source.hashCode();
            result = 31 * result + context.hashCode();
            return result;
        }
    }

    public TopicWords constructTopicWords(){
        return new TopicWords(this.source,this.context,this.target);
    }
    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public String getContextType() {
        return contextType;
    }

    public void setContextType(String contextType) {
        this.contextType = contextType;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public String getConstructor() {
        return constructor;
    }

    public void setConstructor(String constructor) {
        this.constructor = constructor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Reference reference = (Reference) o;

        if (!source.equals(reference.source)) return false;
        if (!context.equals(reference.context)) return false;
        if (!contextType.equals(reference.contextType)) return false;
        if (!target.equals(reference.target)) return false;
        return constructor.equals(reference.constructor);
    }

    @Override
    public int hashCode() {
        int result = source.hashCode();
        result = 31 * result + context.hashCode();
        result = 31 * result + contextType.hashCode();
        result = 31 * result + target.hashCode();
        result = 31 * result + constructor.hashCode();
        return result;
    }
}
