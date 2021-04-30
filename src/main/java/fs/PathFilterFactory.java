package fs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * PathFilter只能作用于文件名过滤，不能针对文件属性（例如创建时间）
 */
public class PathFilterFactory {

    public static PathFilter regexExcludePathFilter(String regex) {
        return new RegexExcludePathFilter(regex);
    }

    /**
     * 排除匹配正则表达式的路径
     */
    static class RegexExcludePathFilter implements PathFilter {
        private String regex;
        public RegexExcludePathFilter(String regex) {
            this.regex = regex;
        }

        /**
         * 判断一个路径是否符合
         * @param path
         * @return
         */
        @Override
        public boolean accept(Path path) {
            return !path.toString().matches(regex);
        }
    }
}
