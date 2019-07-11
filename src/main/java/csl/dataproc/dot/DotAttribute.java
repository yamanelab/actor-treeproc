package csl.dataproc.dot;

import java.util.Arrays;
import java.util.stream.Collectors;

public class DotAttribute {
    public String key;
    public String value;

    public DotAttribute(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String toString() {
        return key + "=" + value;
    }

    public DotAttribute quote() {
        return new DotAttribute(key, quote(value));
    }
    
    public static String quote(String value) {
        StringBuffer buf = new StringBuffer();
        buf.append("\"");
        for (int i = 0, l = value.length(); i < l; ++i) {
            char c = value.charAt(i);
            if (c == '\n') {
                buf.append("\\n");
            } else if (c == '"') {
                buf.append("\\\"");
            } else if (c == '\\') {
                if (i + 1 < l && value.charAt(i + 1) == 'l') { //\l for left align
                    //skip
                    buf.append(c);
                } else {
                    buf.append("\\\\");
                }
            } else {
                buf.append(c);
            }
        }
        buf.append("\"");
        return buf.toString();
    }
    
    public static String unquote(String value) {
        StringBuffer buf = new StringBuffer();
        int start = 0;
        if (value.startsWith("\"")) {
            start++;
        }
        int end = value.length();
        if (value.endsWith("\"")) {
            end--;
        }
        boolean esc = false;
        for (int i = start, l = end; i < l; ++i) {
            char c = value.charAt(i);
            if (!esc) {
                if (c == '\\') {
                    esc = true;
                } else {
                    buf.append(c);
                }
            } else {
                if (c == 'n') {
                    buf.append("\n");
                } else if (c == '"') {
                    buf.append("\"");
                } else if (c == '\\') {
                    buf.append("\\");
                } else {
                    buf.append(c);
                }
                esc = false;
            }
        }
        return buf.toString();        
    }
    
    public static DotAttribute labelQuote(String text) {
        return new DotAttribute("label", text).quote();
    }

    public static DotAttribute colorRGB(int r, int g, int b) {
        return new DotAttribute("color", "\"#" + rgbValue(r) + rgbValue(g) + rgbValue(b) + "\"");
    }

    public static DotAttribute colorBlack() {
        return new DotAttribute("color", "black");
    }

    public static DotAttribute colorLightGray() {
        return new DotAttribute("color", "lightgray");
    }

    public static DotAttribute colorGray() {
        return new DotAttribute("color", "gray");
    }

    public static DotAttribute colorBlue() {
        return new DotAttribute("color", "blue");
    }

    public static DotAttribute colorRed() {
        return new DotAttribute("color", "red");
    }

    private static String rgbValue(int v) {
        String s = Integer.toString(v, 16);
        if (s.length() == 1) {
            s = "0" + s;
        }
        return s;
    }

    public static DotAttribute styleDashed() {
        return new DotAttribute("style", "dashed");
    }
    public static DotAttribute styleDotted() {
        return new DotAttribute("style", "dotted");
    }

    public static DotAttribute styleSolid() {
        return new DotAttribute("style", "solid");
    }

    public static DotAttribute styleBold() {
        return new DotAttribute("style", "bold");
    }

    public static DotAttribute styleRounded() {
        return new DotAttribute("style", "rounded");
    }

    public enum Style {
        Bold, Solid, Dotted, Dashed, Filled, Invisible, Diagonals, Rounded
    }

    public static DotAttribute style(Style... styles) {
        return new DotAttribute("style", Arrays.stream(styles)
                .map(Enum::name)
                .map(String::toLowerCase)
                .collect(Collectors.joining(",")));
    }

    public static DotAttribute arrowDotted(boolean isHead) {
        return new DotAttribute("arrow" + (isHead ? "head" : "tail"), "dotted");
    }

    public static DotAttribute arrowSolid(boolean isHead) {
        return new DotAttribute("arrow" + (isHead ? "head" : "tail"), "solid");
    }

    public static DotAttribute arrowBold(boolean isHead) {
        return new DotAttribute("arrow" + (isHead ? "head" : "tail"), "bold");
    }
    
    public static DotAttribute graphRankDirectionLR() {
        return new DotAttribute("rankdir", "LR");
    }
    
    public static DotAttribute shapeEllipse() { //default
        return new DotAttribute("shape", "ellipse");
    }
    public static DotAttribute shapeBox() {
        return new DotAttribute("shape", "box");
    }
    /**
     * node text color
     */
    public static DotAttribute fontColorRGB(int r, int g, int b) {
        return new DotAttribute("fontcolor", "\"#" + rgbValue(r) + rgbValue(g) + rgbValue(b) + "\"");
    }
    /**
     * node font name
     */
    public static DotAttribute fontName(String fontname) { 
        return new DotAttribute("fontname", fontname).quote();
    }
    /**
     * node font size
     */
    public static DotAttribute fontSize(int n) {
        return new DotAttribute("fontsize", "" + n);
    }
    /**
     * edge label
     */
    public static DotAttribute labelFontColorRGB(int r, int g, int b) {
        return new DotAttribute("labelfontcolor", "\"#" + rgbValue(r) + rgbValue(g) + rgbValue(b) + "\"");
    }
    public static DotAttribute labelFontName(String fontname) { 
        return new DotAttribute("labelfontname", fontname).quote();
    }
    public static DotAttribute labelFontSize(int n) {
        return new DotAttribute("labelfontsize", "" + n);
    }
}
