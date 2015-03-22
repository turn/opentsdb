package net.opentsdb.tsd.expression;

import com.google.common.base.Preconditions;

public class ExprReader {

    protected final char[] chars;

    private int mark = 0;

    public ExprReader(char[] chars) {
        Preconditions.checkNotNull(chars);
        this.chars = chars;
    }

    public int getMark() {
        return mark;
    }

    public char peek() {
        return chars[mark];
    }

    public char next() {
        return chars[mark++];
    }

    public boolean isNextChar(char c) {
        return peek() == c;
    }

    public boolean isNextSeq(CharSequence seq) {
        Preconditions.checkNotNull(seq);
        for (int i=0; i<seq.length(); i++) {
            if (chars[mark+i] != seq.charAt(i)) {
                return false;
            }
        }

        return true;
    }

    public boolean isEOF() {
        return mark == chars.length;
    }

    public void skipWhitespaces() {
        for (int i=mark; i<chars.length; i++) {
            if (Character.isWhitespace(chars[i])) {
                mark++;
            } else {
                break;
            }
        }
    }

}
